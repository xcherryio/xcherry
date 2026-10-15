// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type temporalTestCA struct {
	certificate       *x509.Certificate
	privateKey        *ecdsa.PrivateKey
	caPath            string
	serverCertificate tls.Certificate
}

func newTemporalTestCA(t *testing.T) *temporalTestCA {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	model := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Temporal test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, model, model, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600))
	testCA := &temporalTestCA{certificate: certificate, privateKey: privateKey, caPath: caPath}
	serverCert, serverKey := testCA.issueCertificate(t, "temporal-auth.test", false)
	testCA.serverCertificate, err = tls.X509KeyPair(serverCert, serverKey)
	require.NoError(t, err)
	return testCA
}

func (ca *temporalTestCA) writeClientCertificate(t *testing.T, directory, commonName string) (string, string) {
	t.Helper()
	certificate, key := ca.issueCertificate(t, commonName, true)
	certPath := filepath.Join(directory, "client.crt")
	keyPath := filepath.Join(directory, "client.key")
	require.NoError(t, os.WriteFile(certPath, certificate, 0600))
	require.NoError(t, os.WriteFile(keyPath, key, 0600))
	return certPath, keyPath
}

func (ca *temporalTestCA) issueCertificate(t *testing.T, commonName string, isClient bool) ([]byte, []byte) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	certificate := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: commonName}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	if isClient {
		certificate.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		certificate.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		certificate.DNSNames = []string{commonName}
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, ca.certificate, &privateKey.PublicKey, ca.privateKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
