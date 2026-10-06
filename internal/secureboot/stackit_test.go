// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package secureboot //nolint:testpackage

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stackitcloud/stackit-sdk-go/core/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeKMS answers decrypt requests for ciphertexts produced by encrypt.
type fakeKMS struct {
	t *testing.T
}

func (f *fakeKMS) encrypt(plaintext []byte) string {
	return base64.StdEncoding.EncodeToString(append([]byte("encrypted:"), plaintext...))
}

func (f *fakeKMS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/keyrings/ring/keys/key/versions/3/decrypt") {
		http.Error(w, r.Method+" "+r.URL.Path, http.StatusNotFound)

		return
	}

	var payload struct {
		Data string `json:"data"`
	}

	require.NoError(f.t, json.NewDecoder(r.Body).Decode(&payload))

	ciphertext, err := base64.StdEncoding.DecodeString(payload.Data)
	require.NoError(f.t, err)

	plaintext, ok := strings.CutPrefix(string(ciphertext), "encrypted:")
	if !ok {
		http.Error(w, "bad ciphertext", http.StatusBadRequest)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	require.NoError(f.t, json.NewEncoder(w).Encode(map[string]string{
		"data": base64.StdEncoding.EncodeToString([]byte(plaintext)),
	}))
}

func keyPEM(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()

	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func selfSigned(t *testing.T, key *rsa.PrivateKey) *x509.Certificate {
	t.Helper()

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert
}

func TestStackitKMSService(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())

	signingKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pcrKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	cert := selfSigned(t, signingKey)

	kms := &fakeKMS{t: t}
	server := httptest.NewServer(kms)
	t.Cleanup(server.Close)

	dir := t.TempDir()
	opts := StackitKMSOptions{ProjectID: "project", Region: "eu01", KeyRingID: "ring", KeyID: "key", KeyVersion: 3}

	signingKeyPath := filepath.Join(dir, "signing.key.enc")
	signingCertPath := filepath.Join(dir, "signing.crt")
	pcrKeyPath := filepath.Join(dir, "pcr.key.enc")

	require.NoError(t, os.WriteFile(signingKeyPath, []byte(kms.encrypt(keyPEM(t, signingKey))+"\n"), 0o600))
	require.NoError(t, os.WriteFile(pcrKeyPath, []byte(kms.encrypt(keyPEM(t, pcrKey))), 0o600))
	require.NoError(t, os.WriteFile(signingCertPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0o600))

	kmsOpts := []config.ConfigurationOption{config.WithEndpoint(server.URL), config.WithoutAuthentication()}

	svc, err := NewStackitKMSService(opts, signingKeyPath, signingCertPath, pcrKeyPath, kmsOpts...)
	require.NoError(t, err)

	digest := sha256.Sum256([]byte("boot asset"))

	// the Talos signerd client talks to the in-memory signers
	secureBootSigner, err := svc.in.SecureBootSigner.GetSigner(t.Context())
	require.NoError(t, err)

	assert.True(t, cert.Equal(secureBootSigner.Certificate()))

	signature, err := secureBootSigner.Signer().Sign(rand.Reader, digest[:], crypto.SHA256)
	require.NoError(t, err)
	require.NoError(t, rsa.VerifyPKCS1v15(&signingKey.PublicKey, crypto.SHA256, digest[:], signature))
	require.NoError(t, secureBootSigner.Close())

	pcrSigner, err := svc.in.PCRSigner.GetSigner(t.Context())
	require.NoError(t, err)

	assert.True(t, pcrKey.PublicKey.Equal(pcrSigner.PublicRSAKey()))

	signature, err = pcrSigner.Sign(rand.Reader, digest[:], &rsa.PSSOptions{Hash: crypto.SHA256})
	require.NoError(t, err)
	require.NoError(t, rsa.VerifyPSS(&pcrKey.PublicKey, crypto.SHA256, digest[:], signature, nil))
	require.NoError(t, pcrSigner.Close())

	certPEM, err := svc.GetSecureBootSigningCert()
	require.NoError(t, err)

	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	assert.Equal(t, cert.Raw, block.Bytes)

	// the certificate must belong to the signing key
	otherCertPath := filepath.Join(dir, "other.crt")
	require.NoError(t, os.WriteFile(otherCertPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: selfSigned(t, pcrKey).Raw}), 0o600))

	_, err = NewStackitKMSService(opts, signingKeyPath, otherCertPath, pcrKeyPath, kmsOpts...)
	require.ErrorContains(t, err, "does not match the signing key")
}
