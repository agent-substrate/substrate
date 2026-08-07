// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package substratex509

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/json"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
)

// mintCert self-signs a certificate from the template and parses it back, so
// that ExtraExtensions round-trip into Extensions.
func mintCert(t *testing.T, template *x509.Certificate) *x509.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	template.SerialNumber = big.NewInt(1)
	template.NotBefore = time.Now().Add(-time.Hour)
	template.NotAfter = time.Now().Add(time.Hour)
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing certificate: %v", err)
	}
	return cert
}

// mintCertWithExtension mints a certificate carrying a single PodIdentity
// extension with the given raw value, bypassing the validation in
// AddPodIdentityToCertificate.
func mintCertWithExtension(t *testing.T, value []byte) *x509.Certificate {
	t.Helper()
	return mintCert(t, &x509.Certificate{
		ExtraExtensions: []pkix.Extension{
			{Id: testPodIdentityOID, Value: value},
		},
	})
}

// mintCertWithActorExtension mints a certificate carrying a single
// ActorIdentity extension with the given raw value, bypassing the validation in
// AddActorIdentityToCertificate.
func mintCertWithActorExtension(t *testing.T, value []byte) *x509.Certificate {
	t.Helper()
	return mintCert(t, &x509.Certificate{
		ExtraExtensions: []pkix.Extension{
			{Id: testActorIdentityOID, Value: value},
		},
	})
}

// testPodIdentityOID and testActorIdentityOID spell out the intended extension
// OIDs to prevent accidental modification to the values.
var (
	testPodIdentityOID   = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 11129, 2, 12, 1}
	testActorIdentityOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 11129, 2, 12, 2}
)

func TestPodIdentityFromCertificate(t *testing.T) {
	// fullPodIdentity has every field populated, as required by
	// AddPodIdentityToCertificate. Cases that mutate it copy it first.
	fullPodIdentity := PodIdentity{
		Namespace:          "ate-system",
		ServiceAccountName: "atelet",
		ServiceAccountUID:  "sa-uid",
		PodName:            "atelet-abc",
		PodUID:             "pod-uid",
		NodeName:           "node-1",
		NodeUID:            "node-uid",
	}
	for _, tc := range []struct {
		name    string
		cert    func(t *testing.T) *x509.Certificate
		want    *PodIdentity
		wantErr string // substring of the expected error; "" means no error
	}{
		{
			name: "RoundTrip",
			cert: func(t *testing.T) *x509.Certificate {
				template := &x509.Certificate{}
				if err := AddPodIdentityToCertificate(&fullPodIdentity, template); err != nil {
					t.Fatalf("AddPodIdentityToCertificate: %v", err)
				}
				return mintCert(t, template)
			},
			want: &fullPodIdentity,
		},
		{
			name: "Absent",
			cert: func(t *testing.T) *x509.Certificate {
				return mintCert(t, &x509.Certificate{})
			},
			want: nil,
		},
		{
			name: "Duplicate",
			cert: func(t *testing.T) *x509.Certificate {
				// Go's x509 parser rejects certificates with duplicate
				// extensions, so a duplicate can only reach
				// PodIdentityFromCertificate via a Certificate constructed
				// by other means. Build one directly.
				ext := pkix.Extension{Id: testPodIdentityOID, Value: []byte(`{"PodUID":"pod-uid"}`)}
				return &x509.Certificate{Extensions: []pkix.Extension{ext, ext}}
			},
			wantErr: "multiple PodIdentity extensions",
		},
		{
			name: "EmptyField",
			cert: func(t *testing.T) *x509.Certificate {
				pod := PodIdentity{
					Namespace:          "ate-system",
					ServiceAccountName: "atelet",
					ServiceAccountUID:  "sa-uid",
					PodName:            "atelet-abc",
					NodeName:           "node-1",
					NodeUID:            "node-uid",
				}
				value, err := json.Marshal(pod)
				if err != nil {
					t.Fatalf("marshaling PodIdentity: %v", err)
				}
				return mintCertWithExtension(t, value)
			},
			wantErr: "PodUID",
		},
		{
			name: "Malformed",
			cert: func(t *testing.T) *x509.Certificate {
				return mintCertWithExtension(t, []byte("not json"))
			},
			wantErr: "json-unmarshaling",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := PodIdentityFromCertificate(tc.cert(t))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("PodIdentityFromCertificate succeeded, want error containing %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("PodIdentityFromCertificate error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("PodIdentityFromCertificate: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("PodIdentityFromCertificate = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestAddPodIdentityToCertificateEmptyField(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*PodIdentity)
	}{
		{"Namespace", func(p *PodIdentity) { p.Namespace = "" }},
		{"ServiceAccountName", func(p *PodIdentity) { p.ServiceAccountName = "" }},
		{"ServiceAccountUID", func(p *PodIdentity) { p.ServiceAccountUID = "" }},
		{"PodName", func(p *PodIdentity) { p.PodName = "" }},
		{"PodUID", func(p *PodIdentity) { p.PodUID = "" }},
		{"NodeName", func(p *PodIdentity) { p.NodeName = "" }},
		{"NodeUID", func(p *PodIdentity) { p.NodeUID = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pod := PodIdentity{
				Namespace:          "ate-system",
				ServiceAccountName: "atelet",
				ServiceAccountUID:  "sa-uid",
				PodName:            "atelet-abc",
				PodUID:             "pod-uid",
				NodeName:           "node-1",
				NodeUID:            "node-uid",
			}
			tc.mutate(&pod)
			err := AddPodIdentityToCertificate(&pod, &x509.Certificate{})
			if err == nil {
				t.Fatalf("AddPodIdentityToCertificate succeeded, want error for empty %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.name) {
				t.Errorf("error %q does not name the empty field %s", err, tc.name)
			}
		})
	}
}

func TestActorIdentityFromCertificate(t *testing.T) {
	// fullActorIdentity has every field populated, as required by
	// AddActorIdentityToCertificate. Cases that mutate it copy it first.
	fullActorIdentity := ActorIdentity{
		Atespace:  "team-a",
		ActorName: "researcher",
		ActorUid:  "actor-uid",
		Purpose:   ActorIdentityPurposeAtunnel,
	}
	for _, tc := range []struct {
		name    string
		cert    func(t *testing.T) *x509.Certificate
		want    *ActorIdentity
		wantErr string // substring of the expected error; "" means no error
	}{
		{
			name: "RoundTrip",
			cert: func(t *testing.T) *x509.Certificate {
				template := &x509.Certificate{}
				if err := AddActorIdentityToCertificate(&fullActorIdentity, template); err != nil {
					t.Fatalf("AddActorIdentityToCertificate: %v", err)
				}
				return mintCert(t, template)
			},
			want: &fullActorIdentity,
		},
		{
			name: "Absent",
			cert: func(t *testing.T) *x509.Certificate {
				return mintCert(t, &x509.Certificate{})
			},
			want: nil,
		},
		{
			// A PodIdentity extension must not be mistaken for an
			// ActorIdentity one; the two extensions carry different OIDs.
			name: "PodIdentityOnly",
			cert: func(t *testing.T) *x509.Certificate {
				template := &x509.Certificate{}
				if err := AddPodIdentityToCertificate(&PodIdentity{
					Namespace:          "ate-system",
					ServiceAccountName: "atelet",
					ServiceAccountUID:  "sa-uid",
					PodName:            "atelet-abc",
					PodUID:             "pod-uid",
					NodeName:           "node-1",
					NodeUID:            "node-uid",
				}, template); err != nil {
					t.Fatalf("AddPodIdentityToCertificate: %v", err)
				}
				return mintCert(t, template)
			},
			want: nil,
		},
		{
			name: "Duplicate",
			cert: func(t *testing.T) *x509.Certificate {
				// Go's x509 parser rejects certificates with duplicate
				// extensions, so a duplicate can only reach
				// ActorIdentityFromCertificate via a Certificate constructed
				// by other means. Build one directly.
				ext := pkix.Extension{Id: testActorIdentityOID, Value: []byte(`{"ActorUid":"actor-uid"}`)}
				return &x509.Certificate{Extensions: []pkix.Extension{ext, ext}}
			},
			wantErr: "multiple ActorIdentity extensions",
		},
		{
			name: "EmptyField",
			cert: func(t *testing.T) *x509.Certificate {
				actor := ActorIdentity{
					Atespace:  "team-a",
					ActorName: "researcher",
					Purpose:   ActorIdentityPurposeAtunnel,
				}
				value, err := json.Marshal(actor)
				if err != nil {
					t.Fatalf("marshaling ActorIdentity: %v", err)
				}
				return mintCertWithActorExtension(t, value)
			},
			wantErr: "ActorUid",
		},
		{
			name: "Malformed",
			cert: func(t *testing.T) *x509.Certificate {
				return mintCertWithActorExtension(t, []byte("not json"))
			},
			wantErr: "json-unmarshaling",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ActorIdentityFromCertificate(tc.cert(t))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ActorIdentityFromCertificate succeeded, want error containing %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("ActorIdentityFromCertificate error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ActorIdentityFromCertificate: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ActorIdentityFromCertificate = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestAddActorIdentityToCertificateEmptyField(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ActorIdentity)
	}{
		{"Atespace", func(a *ActorIdentity) { a.Atespace = "" }},
		{"ActorName", func(a *ActorIdentity) { a.ActorName = "" }},
		{"ActorUid", func(a *ActorIdentity) { a.ActorUid = "" }},
		{"Purpose", func(a *ActorIdentity) { a.Purpose = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actor := ActorIdentity{
				Atespace:  "team-a",
				ActorName: "researcher",
				ActorUid:  "actor-uid",
				Purpose:   ActorIdentityPurposeAtunnel,
			}
			tc.mutate(&actor)
			err := AddActorIdentityToCertificate(&actor, &x509.Certificate{})
			if err == nil {
				t.Fatalf("AddActorIdentityToCertificate succeeded, want error for empty %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.name) {
				t.Errorf("error %q does not name the empty field %s", err, tc.name)
			}
		})
	}
}

// The extension value is plain JSON so that non-Go verifiers can parse it
// without an ASN.1 library.
func TestExtensionValueIsJSON(t *testing.T) {
	pod := PodIdentity{
		Namespace:          "ate-system",
		ServiceAccountName: "atelet",
		ServiceAccountUID:  "sa-uid",
		PodName:            "atelet-abc",
		PodUID:             "pod-uid",
		NodeName:           "node-1",
		NodeUID:            "node-uid",
	}
	actor := ActorIdentity{
		Atespace:  "team-a",
		ActorName: "researcher",
		ActorUid:  "actor-uid",
		Purpose:   ActorIdentityPurposeAtunnel,
	}
	template := &x509.Certificate{}
	if err := AddPodIdentityToCertificate(&pod, template); err != nil {
		t.Fatalf("AddPodIdentityToCertificate: %v", err)
	}
	if err := AddActorIdentityToCertificate(&actor, template); err != nil {
		t.Fatalf("AddActorIdentityToCertificate: %v", err)
	}
	cert := mintCert(t, template)

	for _, want := range []struct {
		name string
		oid  asn1.ObjectIdentifier
	}{
		{"PodIdentity", testPodIdentityOID},
		{"ActorIdentity", testActorIdentityOID},
	} {
		found := false
		for _, ext := range cert.Extensions {
			if ext.Id.Equal(want.oid) {
				found = true
				if !json.Valid(ext.Value) {
					t.Errorf("%s extension value is not valid JSON: %q", want.name, ext.Value)
				}
			}
		}
		if !found {
			t.Errorf("%s extension not found in certificate", want.name)
		}
	}
}
