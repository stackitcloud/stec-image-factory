// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package secureboot

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"github.com/siderolabs/crypto/x509"
	"github.com/siderolabs/talos/pkg/imager/profile"
	"github.com/stackitcloud/stackit-sdk-go/core/config"
	kms "github.com/stackitcloud/stackit-sdk-go/services/kms/v1api"
)

// StackitKMSOptions names the STACKIT KMS symmetric key the SecureBoot key files are encrypted with.
//
// The key files contain the base64 ciphertext returned by the KMS encrypt operation.
// Credentials are taken from the STACKIT SDK environment.
type StackitKMSOptions struct {
	ProjectID string
	Region    string
	KeyRingID string
	KeyID     string

	// KeyVersion defaults to 1.
	KeyVersion int64
}

// NewStackitKMSService is the file-based approach with the private keys encrypted by STACKIT KMS:
// the keys are decrypted at startup, kept in memory and served to the imager over its signer sockets.
func NewStackitKMSService(opts StackitKMSOptions, signingKeyPath, signingCertPath, pcrKeyPath string, kmsOpts ...config.ConfigurationOption) (*Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	client, err := kms.NewAPIClient(kmsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create STACKIT KMS client: %w", err)
	}

	version := opts.KeyVersion
	if version == 0 {
		version = 1
	}

	decryptKey := func(path string) (*rsa.PrivateKey, error) {
		ciphertext, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		decrypted, err := client.DefaultAPI.Decrypt(ctx, opts.ProjectID, opts.Region, opts.KeyRingID, opts.KeyID, version).
			DecryptPayload(kms.DecryptPayload{Data: string(bytes.TrimSpace(ciphertext))}).Execute()
		if err != nil {
			return nil, fmt.Errorf("STACKIT KMS decrypt of %s: %w", path, err)
		}

		keyPEM, err := base64.StdEncoding.DecodeString(decrypted.Data)
		if err != nil {
			return nil, fmt.Errorf("decode decrypted %s: %w", path, err)
		}

		key, err := (&x509.PEMEncodedCertificateAndKey{Key: keyPEM}).GetRSAKey()
		if err != nil {
			return nil, fmt.Errorf("decrypted %s: %w", path, err)
		}

		return key, nil
	}

	signingKey, err := decryptKey(signingKeyPath)
	if err != nil {
		return nil, err
	}

	pcrKey, err := decryptKey(pcrKeyPath)
	if err != nil {
		return nil, err
	}

	certPEM, err := os.ReadFile(signingCertPath)
	if err != nil {
		return nil, err
	}

	cert, err := (&x509.PEMEncodedCertificate{Crt: certPEM}).GetCert()
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", signingCertPath, err)
	}

	if certKey, ok := cert.PublicKey.(*rsa.PublicKey); !ok || !certKey.Equal(&signingKey.PublicKey) {
		return nil, fmt.Errorf("%s does not match the signing key", signingCertPath)
	}

	secureBootAddr, err := serveSigner("secureboot", &signerServer{key: signingKey, cert: cert})
	if err != nil {
		return nil, err
	}

	pcrAddr, err := serveSigner("pcr", &signerServer{key: pcrKey})
	if err != nil {
		return nil, err
	}

	return &Service{
		in: &profile.SecureBootAssets{
			SecureBootSigner: profile.SigningKeyAndCertificate{SignerAddress: secureBootAddr},
			PCRSigner:        profile.SigningKey{SignerAddress: pcrAddr},
		},
	}, nil
}
