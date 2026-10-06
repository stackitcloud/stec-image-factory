// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package secureboot

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"

	"github.com/siderolabs/talos/pkg/machinery/api/signer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// signerServer serves an in-memory RSA key over the Talos SignerService API.
type signerServer struct {
	signer.UnimplementedSignerServiceServer

	key  *rsa.PrivateKey
	cert *x509.Certificate
}

func (s *signerServer) GetCertificate(context.Context, *emptypb.Empty) (*signer.CertificateResponse, error) {
	if s.cert == nil {
		return nil, status.Error(codes.NotFound, "signer has no certificate")
	}

	return &signer.CertificateResponse{
		CertPem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.cert.Raw}),
	}, nil
}

func (s *signerServer) GetPublicKey(context.Context, *emptypb.Empty) (*signer.PublicKeyResponse, error) {
	der, err := x509.MarshalPKIXPublicKey(&s.key.PublicKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal public key: %v", err)
	}

	return &signer.PublicKeyResponse{
		PubKeyPem: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}),
	}, nil
}

func (s *signerServer) Sign(_ context.Context, req *signer.SignRequest) (*signer.SignResponse, error) {
	var hash crypto.Hash

	switch req.GetHash() {
	case signer.Hash_HASH_SHA256:
		hash = crypto.SHA256
	case signer.Hash_HASH_SHA384:
		hash = crypto.SHA384
	case signer.Hash_HASH_SHA512:
		hash = crypto.SHA512
	case signer.Hash_HASH_UNSPECIFIED:
		fallthrough
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported hash %s", req.GetHash())
	}

	var (
		signature []byte
		err       error
	)

	switch req.GetScheme() {
	case signer.Scheme_SCHEME_RSA_PKCS1V15:
		signature, err = rsa.SignPKCS1v15(rand.Reader, s.key, hash, req.GetDigest())
	case signer.Scheme_SCHEME_RSA_PSS:
		signature, err = rsa.SignPSS(rand.Reader, s.key, hash, req.GetDigest(), &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	case signer.Scheme_SCHEME_UNSPECIFIED:
		fallthrough
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported scheme %s", req.GetScheme())
	}

	if err != nil {
		return nil, status.Errorf(codes.Internal, "sign: %v", err)
	}

	return &signer.SignResponse{Signature: signature}, nil
}

// serveSigner serves srv for the lifetime of the process on a unix socket
// below the temp dir and returns the address for the imager profile.
//
// The path is fixed so that it does not change the profile hash between restarts.
func serveSigner(name string, srv *signerServer) (string, error) {
	path := filepath.Join(os.TempDir(), "image-factory-signer", name+".sock")

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}

	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}

	lis, err := net.Listen("unix", path)
	if err != nil {
		return "", err
	}

	server := grpc.NewServer()
	signer.RegisterSignerServiceServer(server, srv)

	go server.Serve(lis) //nolint:errcheck

	return "unix://" + path, nil
}
