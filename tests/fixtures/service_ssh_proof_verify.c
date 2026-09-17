/* Native WOS test linked against the built Dropbear objects (except dbmulti).
 * Generates ephemeral keys in memory; never reads or writes an installed key.
 */
#include "auth.h"
#include "crypto_desc.h"
#include "dbrandom.h"
#include "dbutil.h"
#include "ecdsa.h"
#include "gened25519.h"
#include "genrsa.h"
#include "includes.h"
#include "ssh.h"

static unsigned checks, failures;

static void check(int pass, const char* name) {
    checks++;
    if (!pass) {
        failures++;
        printf("FAIL proof verifier: %s\n", name);
    }
}

static void exercise(sign_key* private_key, enum signature_type sigtype) {
    enum signkey_type keytype = signkey_type_from_signature(sigtype);
    sign_key* public_key = new_sign_key();
    buffer* pub = buf_new(MAX_PUBKEY_SIZE);
    buffer* sid = buf_new(32);
    buffer* request = buf_new(16384);
    buffer* transcript;
    buffer* saved;
    unsigned int prefix = 7, signature_pos, original_len, pub_len;
    unsigned int i;
    const unsigned char* pub_data;
    const char* algorithm = signature_name_from_type(sigtype, NULL);

    buf_put_pub_key(pub, private_key, keytype);
    buf_setpos(pub, 4);
    pub_len = pub->len - 4;
    pub_data = buf_getptr(pub, pub_len);
    check(buf_get_pub_key(pub, public_key, &keytype) == DROPBEAR_SUCCESS, "decode public key");
    for (i = 0; i < 32; i++) {
        buf_putbyte(sid, i);
    }
    /* A nonzero packet start matches the uncompressed network packet path. */
    for (i = 0; i < prefix; i++) {
        buf_putbyte(request, 0xa5);
    }
    buf_putbyte(request, SSH_MSG_USERAUTH_REQUEST);
    buf_putstring(request, "fixture", 7);
    buf_putstring(request, SSH_SERVICE_CONNECTION, SSH_SERVICE_CONNECTION_LEN);
    buf_putstring(request, AUTH_METHOD_PUBKEY, AUTH_METHOD_PUBKEY_LEN);
    buf_putbyte(request, 1);
    buf_putstring(request, algorithm, strlen(algorithm));
    buf_putstring(request, (const char*)pub_data, pub_len);
    signature_pos = request->pos;
    transcript = buf_new(4 + sid->len + signature_pos - prefix);
    buf_putbufstring(transcript, sid);
    buf_setpos(request, prefix);
    buf_putbytes(transcript, buf_getptr(request, signature_pos - prefix), signature_pos - prefix);
    buf_setpos(request, signature_pos);
    buf_put_sign(request, private_key, sigtype, transcript);
    original_len = request->len;
    saved = buf_newcopy(request);
    buf_setpos(saved, request->pos);

#define VERIFY(account, session, blob, bloblen, type) \
    svr_verify_pubkey_auth(request, prefix, session, account, blob, bloblen, public_key, type)
    check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_SUCCESS, "valid proof");
    check(request->pos == saved->pos && request->len == saved->len && memcmp(request->data, saved->data, request->len) == 0,
          "request unchanged");
    check(VERIFY("different-account", sid, pub_data, pub_len, sigtype) == DROPBEAR_FAILURE, "trusted account mismatch");
    check(VERIFY("fixture", sid, pub_data, pub_len - 1, sigtype) == DROPBEAR_FAILURE, "authorized key mismatch");
    check(VERIFY("fixture", sid, pub_data, pub_len, DROPBEAR_SIGNATURE_NONE) == DROPBEAR_FAILURE, "invalid algorithm");
    sid->data[0] ^= 1;
    check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_FAILURE, "trusted session mismatch");
    sid->data[0] ^= 1;
    /* Every truncated view must reject without entering a fatal buffer read. */
    for (i = prefix; i < original_len; i++) {
        request->len = i;
        check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_FAILURE, "truncated request");
    }
    request->len = original_len;
    buf_setpos(request, signature_pos);
    buf_putint(request, 0);
    check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_FAILURE, "zero outer length");
    buf_setpos(request, signature_pos);
    buf_putint(request, UINT_MAX);
    check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_FAILURE, "overflow outer length");
    memcpy(request->data, saved->data, original_len);
    buf_setpos(request, original_len);
    buf_putbyte(request, 0);
    check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_FAILURE, "trailing packet byte");
    buf_setpos(request, signature_pos);
    buf_putint(request, original_len - signature_pos - 4 + 1);
    check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_FAILURE, "trailing signature byte");
    memcpy(request->data, saved->data, original_len);
    request->len = original_len;
    request->data[original_len - 1] ^= 1;
    check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_FAILURE, "corrupt signature");
    request->data[original_len - 1] ^= 1;
    check(VERIFY("fixture", sid, pub_data, pub_len, sigtype) == DROPBEAR_SUCCESS, "valid after denials");
#undef VERIFY
    printf("proof verifier %s complete\n", algorithm);
    buf_free(saved);
    buf_free(transcript);
    buf_free(request);
    buf_free(sid);
    buf_free(pub);
    sign_key_free(public_key);
}

int main(void) {
    sign_key* key;
    crypto_init();
    seedrandom();
    key = new_sign_key();
    key->ed25519key = gen_ed25519_priv_key(256);
    exercise(key, DROPBEAR_SIGNATURE_ED25519);
    sign_key_free(key);
    key = new_sign_key();
    key->rsakey = gen_rsa_priv_key(2048);
    exercise(key, DROPBEAR_SIGNATURE_RSA_SHA256);
    sign_key_free(key);
    key = new_sign_key();
    key->ecckey256 = gen_ecdsa_priv_key(256);
    exercise(key, DROPBEAR_SIGNATURE_ECDSA_NISTP256);
    sign_key_free(key);
    printf("proof verifier: %u checks, %u failed\n", checks, failures);
    return failures != 0;
}
