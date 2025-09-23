package clickhouse

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// If you ever need to regenerate these
//
//openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 \
//  -out ca.crt \
//  -subj "/C=AB/ST=CD/L=SomeRock/O=TestCA/CN=Test Root CA"
//
// openssl genrsa -out tls.key 2048
//
// openssl req -new -key tls.key -out tls.csr \
//  -subj "/C=AB/ST=CD/L=TheMoon/O=TestOrg/CN=foobar"
//
// openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
// -out tls.crt -days 365 -sha256

func TestGetTLSConfig(t *testing.T) {
	// Create temporary test certificates
	tmpDir := t.TempDir()

	certData := []byte(`-----BEGIN CERTIFICATE-----
MIIDIDCCAggCCQCOQn0iuLBAPTANBgkqhkiG9w0BAQsFADBVMQswCQYDVQQGEwJB
QjELMAkGA1UECAwCQ0QxETAPBgNVBAcMCFNvbWVSb2NrMQ8wDQYDVQQKDAZUZXN0
Q0ExFTATBgNVBAMMDFRlc3QgUm9vdCBDQTAeFw0yNTA5MjIyMDQ4NTNaFw0yNjA5
MjIyMDQ4NTNaME8xCzAJBgNVBAYTAkFCMQswCQYDVQQIDAJDRDEQMA4GA1UEBwwH
VGhlTW9vbjEQMA4GA1UECgwHVGVzdE9yZzEPMA0GA1UEAwwGZm9vYmFyMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApztMnssD8+32bnlH0IAaXGkfO6aN
Stye+AMIzdJRIKjFy8ka87xaF5DDMEes0v88aA1kBm8HEApjxX2CDLcK64m/OAvJ
R60O/EeIXaoq7/tTaFN/XX7svZ+IIoj+YW2IjMoe9pvHSJ1ZAsUKca6tCzb85rAs
/EKhEv4VMVfLiXaoudgtvNbliHr0YuDzRkBjok4SbVML2g4lnZJv9lNbp22frqYJ
R2KTVNpc949keTl6acOqtruCV3mpRdYoWs6D9rz+0Z6JXkQl4H9n4RWDhTbJY9ou
vb2bDsm0qSdblLuVzep7b6XE9l49McjyuSi901pfdhFUpn2A3Zh3KRk3fQIDAQAB
MA0GCSqGSIb3DQEBCwUAA4IBAQB2VH72/5j74fyDoudPoGXFEwezrgkQTZVrd3UI
IwCB29l+MNu/jhbkOyccwgHmgRjJ5MkVNmJYJoAqu+JTeMONsUnat7hLq8cK0nmZ
fhACM7wwqQ6cwsH3atytsiuPqov9N4ba8/FN0EmMGpCyOQPzP8jpSFt0dSSyQE7O
MixjKpq6MBSZ09jB0vozVo2O2vAdn1bNCYsj5NHxVpFJCHXg5eulAysQJPfaRhLw
6jaxQvoXNBjxbyRAh9ef2NNsZwt8xyPa8tXAeddSFuTre+B+FU3es/Gvu4gvSzC/
sqBS3SQhjbKZh4rjqBFKqy1kvj6pCMYK74ijwBX6U7muSn4Z
-----END CERTIFICATE-----
	`)

	keyData := []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEpgIBAAKCAQEApztMnssD8+32bnlH0IAaXGkfO6aNStye+AMIzdJRIKjFy8ka
87xaF5DDMEes0v88aA1kBm8HEApjxX2CDLcK64m/OAvJR60O/EeIXaoq7/tTaFN/
XX7svZ+IIoj+YW2IjMoe9pvHSJ1ZAsUKca6tCzb85rAs/EKhEv4VMVfLiXaoudgt
vNbliHr0YuDzRkBjok4SbVML2g4lnZJv9lNbp22frqYJR2KTVNpc949keTl6acOq
truCV3mpRdYoWs6D9rz+0Z6JXkQl4H9n4RWDhTbJY9ouvb2bDsm0qSdblLuVzep7
b6XE9l49McjyuSi901pfdhFUpn2A3Zh3KRk3fQIDAQABAoIBAQCD7Rb76mJ6685N
xqWtyXGCV3fJQhIS7csHErW+UV2o6xvnMQZvI7bbAlvJWbHwXmRUHcQ8AxjKQbgF
04Zl63zU5b0RRNMMFW1N/kyIv7bRzS42jjBbHMc8rH7j57juOU/Q6Z7Vo3zqPx86
PSGJH2jqAP4wKunCM/oIFqO3YnByRv+Z/jh51ABmTxLk3madNT+7/IpB9yv2HExE
3GFxk2b64VYAOWED3QqMJ+1bxsPRsmeazVJSUBy+x61EcGHDaEqvEwPO7H1i9EAJ
OoHzd838n0sOyLBKzy3pSOXCSp/N8pKDBl8JfGCz3aFw9jnR1ZJE9DqWaaRzhlGn
eWoda4aNAoGBANzmkpBRhDYI/MLoSh5PppLcJuWuTXOBTcg8ZpSHYRwsaON+ksZQ
ah5uFdr4S8aXnnU5AvmP7xmlU8cDz7K2wnxXNzYwvSAv7RPUfSPOx2BMKpsEHEGm
nlKMjswBqMF1VLi90ZIzhKMAPvoPe+gnPlgdjVNT6LukedDWSjad0AdLAoGBAMHN
quvYuFrD/YV2XTLMDEQR1gmaP+FQ9htjtpQG/oZqPvxXnImA11ayLghb3gwwiDkD
J32QLNhUPqa3CDTYjEjJ5PzaTBSdxECPM89pxACCp+nz/1m+oDQAhnbaFmqBeGZO
k5cfwowYvd+oznN6Z1qkDp45xr2sWo/HtmzB3RdXAoGBAJygT+NMKTY3ASEhmwwK
5czTlDDeecQgNop3aTR/GPfk9Sn4oRFECzowTAbKbtAMySnMlrNkvt+sPc6qgSXy
N8v5+wfNXwvnb23UCDbPcsZwuvTW4UIbZb6aMtWrh4BENSIYVPQY2z7H+d9MA0sE
KExKB97BbA8/+697gXtgQhQpAoGBAII1rKDI+xQAmlURMYzvi04wrZ3GBSVdP/0n
AYRzLo8g0zC3rHS2G/gjvsne71FBSkUD2YSDWZ3+/BkG4HyhAZ/mBfPct0Eivrih
fi3c4dN19Vs6lFW3vesybyVrYaQtWLdkE8V1kUCPXwLmZ88ubkJ391pXdy/4nN/9
TpPDrMgBAoGBAImZbWLofoGLeWBbARRtNMFiUqiGeGeGBxEOf+QtKIiKJbGlOXH0
/aJCOq+ZkVD2/MZFwfaZ4a7BvNugp13m3tWnsWI6eBnz6pvP0V5SnoKTlzqMzFQC
+vyveBSNRAPUXnN+ei6qx2DA317+8h/i3Aq7VFYCB/8qIRBocxwBun35
-----END RSA PRIVATE KEY-----
	`)

	caData := []byte(`-----BEGIN CERTIFICATE-----
MIIDJjCCAg4CCQCdqt0WR2Mh2jANBgkqhkiG9w0BAQsFADBVMQswCQYDVQQGEwJB
QjELMAkGA1UECAwCQ0QxETAPBgNVBAcMCFNvbWVSb2NrMQ8wDQYDVQQKDAZUZXN0
Q0ExFTATBgNVBAMMDFRlc3QgUm9vdCBDQTAeFw0yNTA5MjIyMDQ1MjBaFw0yNjA5
MjIyMDQ1MjBaMFUxCzAJBgNVBAYTAkFCMQswCQYDVQQIDAJDRDERMA8GA1UEBwwI
U29tZVJvY2sxDzANBgNVBAoMBlRlc3RDQTEVMBMGA1UEAwwMVGVzdCBSb290IENB
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApDc+o6mWk+NkP5NMkwp8
JW2XP4wjb0Fdfz1vwX22FokTK0DzOhgLtj4qkpmQIoYcNkPy/2A8AjYidbA7zchT
7a0xaWmmtr24tlINQ3pmlv5E6RMryelf8JgOuLGUajOIQK9YuUsXCco06zjdwOt1
N4fy8C0SyCtZGI/Ezzuo+xjDB8A3AktE21xG8J70Hwt/ztcSYkC4RqhK6LAoyHvZ
I+A7Ddnmo08MBqYCfjPqSohSLnEk1MSZfKf8Luymbx+PHEt+X/x/fp0VhOxTPpPV
6xT3WQ639XJe8VySJixkCxFPfVpx7F+VLGhWzelMx6dtebuFvbVBOcc9k6gxHclh
jwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBwAdqhH4ZYku6Ivmsq5uSWAJiJ3rpk
hUFU/u/KHw+RHkjwYxG3pxcHdW1ckG3/yWVBQ6LdMQHRB7TJsGFT2YA30Xy4QIX0
Qb9RQpy1MOP92FbxkuKznogX+FfQ6lFMQg5GEythjuSGf2/Lbrk8jEPL0By7xdzO
s4/ZKma7+9MDTIHBSJjUmA7PqiRWSYvLda4+NhCpQ074mhI+fIU2PU+TbrljKaHg
NuxeKZXMBIQP1XoQm+nAd2qFpx2SHrjQyIuJ+9Ee9d5GRIjNt9FyNdjeM/hwoLTF
zoXaxemNp3beJOgrWxT3lWeeBubYnSS3M6Gi074wswKwt4ag1r8T7jsL
-----END CERTIFICATE-----
	`)

	certFile := filepath.Join(tmpDir, "cert.tls")
	keyFile := filepath.Join(tmpDir, "cert.key")
	caFile := filepath.Join(tmpDir, "ca.tls")

	require.NoError(t, os.WriteFile(certFile, certData, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyData, 0o600))
	require.NoError(t, os.WriteFile(caFile, caData, 0o600))

	tests := []struct {
		name    string
		opts    ClientOptions
		wantErr bool
	}{
		{
			name: "valid configuration",
			opts: ClientOptions{
				TLSSettings: TLSSettings{
					CertFile: certFile,
					KeyFile:  keyFile,
					CAFile:   caFile,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid cert file",
			opts: ClientOptions{
				TLSSettings: TLSSettings{
					CertFile: "bogus.tls",
					KeyFile:  keyFile,
					CAFile:   caFile,
				},
			},
			wantErr: true,
		},
		{
			name: "invalid key file",
			opts: ClientOptions{
				TLSSettings: TLSSettings{
					CertFile: certFile,
					KeyFile:  "bogus.key",
					CAFile:   caFile,
				},
			},
			wantErr: true,
		},
		{
			name: "invalid CA file",
			opts: ClientOptions{
				TLSSettings: TLSSettings{
					CertFile: certFile,
					KeyFile:  keyFile,
					CAFile:   "bogus.tls",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := GetTLSConfig(tt.opts)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, cfg)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cfg)
			require.Len(t, cfg.Certificates, 1)
			require.NotNil(t, cfg.RootCAs)
		})
	}
}
