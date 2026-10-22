// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package cmd

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/siderolabs/image-factory/internal/secureboot"
)

// buildSecureBootServiceWithStackit prefers the STACKIT KMS backed SecureBoot service when it is
// configured and falls back to the upstream backends otherwise.
func buildSecureBootServiceWithStackit(ctx context.Context, logger *zap.Logger, opts Options) (*secureboot.Service, error) {
	if !opts.SecureBoot.Enabled || opts.SecureBoot.STACKITKMS.KeyID == "" {
		return buildSecureBootService(opts)
	}

	svc, err := secureboot.NewStackitKMSService(
		ctx,
		logger,
		secureboot.StackitKMSOptions(opts.SecureBoot.STACKITKMS),
		opts.SecureBoot.File.SigningKeyPath,
		opts.SecureBoot.File.SigningCertPath,
		opts.SecureBoot.File.PCRKeyPath,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize SecureBoot service: %w", err)
	}

	return svc, nil
}
