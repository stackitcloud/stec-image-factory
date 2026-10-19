// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package secureboot

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/awnumar/memguard"
	sideroX509 "github.com/siderolabs/crypto/x509"
	"github.com/siderolabs/talos/pkg/imager/profile"
	"github.com/stackitcloud/stackit-sdk-go/core/config"
	kms "github.com/stackitcloud/stackit-sdk-go/services/kms/v1api"
	"go.uber.org/zap"
)

// StackitKMSOptions names the STACKIT KMS symmetric key the SecureBoot key files are encrypted with.
//
// The key files contain the base64 ciphertext returned by the KMS encrypt operation.
// Credentials are taken from the STACKIT SDK environment.
type StackitKMSOptions struct {
	ProjectID  string
	Region     string
	KeyRingID  string
	KeyID      string
	KeyVersion int64
}

// NewStackitKMSService is the file-based approach with the private keys encrypted by STACKIT KMS:
// the keys are decrypted at startup, sealed in memory and served to the imager over its signer sockets.
func NewStackitKMSService(
	logger *zap.Logger,
	opts StackitKMSOptions,
	signingKeyPath, signingCertPath, pcrKeyPath string,
	kmsOpts ...config.ConfigurationOption,
) (*Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	client, err := kms.NewAPIClient(kmsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create STACKIT KMS client: %w", err)
	}

	signingKey, err := decryptKey(ctx, client, opts, signingKeyPath)
	if err != nil {
		return nil, err
	}

	pcrKey, err := decryptKey(ctx, client, opts, pcrKeyPath)
	if err != nil {
		return nil, err
	}

	certPEM, err := os.ReadFile(signingCertPath)
	if err != nil {
		return nil, err
	}

	cert, err := (&sideroX509.PEMEncodedCertificate{Crt: certPEM}).GetCert()
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", signingCertPath, err)
	}

	if certKey, ok := cert.PublicKey.(*rsa.PublicKey); !ok || !certKey.Equal(signingKey.public) {
		return nil, fmt.Errorf("%s does not match the signing key", signingCertPath)
	}

	secureBootAddr, err := serveSigner("secureboot", &signerServer{
		logger: logger.With(zap.String("signer", "secureboot")),
		key:    signingKey,
		cert:   cert,
	})
	if err != nil {
		return nil, err
	}

	pcrAddr, err := serveSigner("pcr", &signerServer{
		logger: logger.With(zap.String("signer", "pcr")),
		key:    pcrKey,
	})
	if err != nil {
		return nil, err
	}

	logger.Info("SecureBoot keys decrypted with STACKIT KMS and sealed in memory",
		zap.String("project_id", opts.ProjectID),
		zap.String("region", opts.Region),
		zap.String("key_ring_id", opts.KeyRingID),
		zap.String("key_id", opts.KeyID),
		zap.Int64("key_version", opts.KeyVersion),
		zap.String("secureboot_signer", secureBootAddr),
		zap.String("pcr_signer", pcrAddr),
		zap.String("signing_cert_subject", cert.Subject.String()),
	)

	return &Service{
		in: &profile.SecureBootAssets{
			SecureBootSigner: profile.SigningKeyAndCertificate{SignerAddress: secureBootAddr},
			PCRSigner:        profile.SigningKey{SignerAddress: pcrAddr},
		},
	}, nil
}

// decryptKey decrypts the KMS ciphertext at path and seals the RSA private key it contains.
func decryptKey(ctx context.Context, client *kms.APIClient, opts StackitKMSOptions, path string) (*sealedKey, error) {
	ciphertext, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	decrypted, err := client.DefaultAPI.Decrypt(ctx, opts.ProjectID, opts.Region, opts.KeyRingID, opts.KeyID, opts.KeyVersion).
		DecryptPayload(kms.DecryptPayload{Data: string(bytes.TrimSpace(ciphertext))}).Execute()
	if err != nil {
		return nil, fmt.Errorf("STACKIT KMS decrypt of %s: %w", path, err)
	}

	keyPEM, err := base64.StdEncoding.DecodeString(decrypted.Data)
	if err != nil {
		return nil, fmt.Errorf("decode decrypted %s: %w", path, err)
	}

	defer memguard.WipeBytes(keyPEM)

	block, _ := pem.Decode(keyPEM)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		return nil, fmt.Errorf("decrypted %s: expected a PEM encoded RSA PRIVATE KEY", path)
	}

	key, err := sealKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("decrypted %s: %w", path, err)
	}

	return key, nil
}
