#!/bin/bash
# Transfer SOL from one account to another.
#
# Defaults are wired to the requested transfer (0.05 SOL between two addresses),
# but all values can be overridden via flags/env. The signing keypair (private
# key) for the SENDER must be supplied.
#
# SECURITY WARNING: a private key passed as a CLI argument is visible in your
# shell history and in `ps`. Prefer PRIVATE_KEY env var or a keypair file.
#
# Backends (auto-detected): prefers the `solana` CLI; otherwise builds, signs,
# and submits a legacy transaction with Node (native ed25519, zero deps).
#
# Usage:
#   ./transfer_sol.sh <PRIVATE_KEY>                 # uses built-in from/to/amount
#   PRIVATE_KEY=... ./transfer_sol.sh               # key via env (safer)
#   ./transfer_sol.sh --from ADDR --to ADDR --amount 0.05 <PRIVATE_KEY>
#   ./transfer_sol.sh --dry-run <PRIVATE_KEY>       # build+sign, DO NOT broadcast
#   RPC_URL=https://api.devnet.solana.com ./transfer_sol.sh <PRIVATE_KEY>
#
# PRIVATE_KEY may be base58 (Phantom export) or a JSON byte array ([12,34,...]).
set -euo pipefail

FROM="${FROM:-DFagbpsNcfmDMYjR5FS9o5E79e1puWF1sSdHkR9eVaPU}"
TO="${TO:-4YPYNbYdSUBbVtiyGznEePpHBoRiGRP2ZMwk5dnuYjcb}"
AMOUNT_SOL="${AMOUNT_SOL:-0.05}"
RPC_URL="${RPC_URL:-http://127.0.0.1:3360}"   # default: local Smart Router (proxies testnet)
# Explorer links can't infer the cluster from a localhost router URL; override if needed.
CLUSTER="${CLUSTER:-testnet}"
PRIVATE_KEY="${PRIVATE_KEY:-}"
DRY_RUN="${DRY_RUN:-0}"

# --- parse flags; first non-flag positional is the private key ---
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --from)    FROM="$2"; shift 2 ;;
        --to)      TO="$2"; shift 2 ;;
        --amount)  AMOUNT_SOL="$2"; shift 2 ;;
        --rpc)     RPC_URL="$2"; shift 2 ;;
        --dry-run) DRY_RUN="1"; shift ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *)         PRIVATE_KEY="$1"; shift ;;
    esac
done

if [[ -z "$PRIVATE_KEY" ]]; then
    echo "ERROR: sender private key required (CLI arg or PRIVATE_KEY env)." >&2
    echo "       Run with --help for usage." >&2
    exit 1
fi

echo "============================================"
echo "Transfer SOL"
echo "============================================"
echo "From:    $FROM"
echo "To:      $TO"
echo "Amount:  $AMOUNT_SOL SOL"
echo "RPC:     $RPC_URL"
[[ "$DRY_RUN" == "1" ]] && echo "Mode:    DRY RUN (build + sign only, no broadcast)"
echo "============================================"
echo ""

# ---------------------------------------------------------------------------
# Backend 1: solana CLI (canonical)
# ---------------------------------------------------------------------------
if command -v solana >/dev/null 2>&1; then
    echo "(using solana CLI)"
    KEYFILE=$(mktemp)
    trap 'rm -f "$KEYFILE"' EXIT
    # Normalize the key into an id.json (JSON byte array) the CLI understands.
    if [[ "$PRIVATE_KEY" == \[* ]]; then
        printf '%s' "$PRIVATE_KEY" > "$KEYFILE"
    else
        # base58 -> JSON array via node (CLI keygen can't import base58 directly).
        PRIVATE_KEY="$PRIVATE_KEY" node -e '
          const A="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
          let b=[0]; for(const c of process.env.PRIVATE_KEY){const p=A.indexOf(c);
            if(p<0)throw new Error("bad base58");let car=p;
            for(let j=0;j<b.length;j++){car+=b[j]*58;b[j]=car&255;car=Math.floor(car/256);}
            while(car>0){b.push(car&255);car=Math.floor(car/256);}}
          for(let i=0;i<process.env.PRIVATE_KEY.length&&process.env.PRIVATE_KEY[i]==="1";i++)b.push(0);
          process.stdout.write(JSON.stringify(b.reverse()));' > "$KEYFILE"
    fi
    if [[ "$DRY_RUN" == "1" ]]; then
        solana transfer --from "$KEYFILE" "$TO" "$AMOUNT_SOL" --url "$RPC_URL" \
            --allow-unfunded-recipient --fee-payer "$KEYFILE" --dump-transaction-message --sign-only
    else
        solana transfer --from "$KEYFILE" "$TO" "$AMOUNT_SOL" --url "$RPC_URL" \
            --allow-unfunded-recipient --fee-payer "$KEYFILE"
    fi
    exit $?
fi

# ---------------------------------------------------------------------------
# Backend 2: Node (build + sign + send a legacy transaction, zero deps)
# ---------------------------------------------------------------------------
echo "(solana CLI not found — using node native ed25519 signer)"
echo ""

FROM="$FROM" TO="$TO" AMOUNT_SOL="$AMOUNT_SOL" RPC_URL="$RPC_URL" CLUSTER="$CLUSTER" \
PRIVATE_KEY="$PRIVATE_KEY" DRY_RUN="$DRY_RUN" node - <<'NODE'
const crypto = require('crypto');

const ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

function b58decode(str) {
  let bytes = [0];
  for (const c of str) {
    const p = ALPHABET.indexOf(c);
    if (p < 0) throw new Error('invalid base58 char: ' + c);
    let carry = p;
    for (let j = 0; j < bytes.length; j++) {
      carry += bytes[j] * 58;
      bytes[j] = carry & 0xff;
      carry = Math.floor(carry / 256);
    }
    while (carry > 0) { bytes.push(carry & 0xff); carry = Math.floor(carry / 256); }
  }
  for (let i = 0; i < str.length && str[i] === '1'; i++) bytes.push(0);
  return Buffer.from(bytes.reverse());
}

function b58encode(buf) {
  let zeros = 0;
  while (zeros < buf.length && buf[zeros] === 0) zeros++;
  const digits = [0];
  for (let i = zeros; i < buf.length; i++) {
    let carry = buf[i];
    for (let j = 0; j < digits.length; j++) {
      carry += digits[j] << 8;
      digits[j] = carry % 58;
      carry = (carry / 58) | 0;
    }
    while (carry > 0) { digits.push(carry % 58); carry = (carry / 58) | 0; }
  }
  let out = '1'.repeat(zeros);
  for (let k = digits.length - 1; k >= 0; k--) out += ALPHABET[digits[k]];
  return out;
}

// compact-u16 (shortvec) length prefix used by Solana for arrays.
function encodeLength(len) {
  const out = [];
  let rem = len;
  for (;;) {
    let elem = rem & 0x7f;
    rem >>= 7;
    if (rem === 0) { out.push(elem); break; }
    out.push(elem | 0x80);
  }
  return Buffer.from(out);
}

// Parse base58 or JSON-array secret into {seed(32), pub(32)}.
function parseSecret(raw) {
  raw = raw.trim();
  let secret = raw.startsWith('[') ? Buffer.from(JSON.parse(raw)) : b58decode(raw);
  if (secret.length === 64) return { seed: secret.subarray(0, 32), pub: secret.subarray(32, 64) };
  if (secret.length === 32) {
    const der = Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), secret]);
    const sk = crypto.createPrivateKey({ key: der, format: 'der', type: 'pkcs8' });
    const x = crypto.createPublicKey(sk).export({ format: 'jwk' }).x;
    return { seed: secret, pub: Buffer.from(x, 'base64url') };
  }
  throw new Error('private key must decode to 32 or 64 bytes, got ' + secret.length);
}

function signerFromSeed(seed) {
  const der = Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), seed]);
  return crypto.createPrivateKey({ key: der, format: 'der', type: 'pkcs8' });
}

async function rpc(url, method, params) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  const json = await res.json();
  if (json.error) throw new Error(`${method} RPC error: ${JSON.stringify(json.error)}`);
  return json.result;
}

(async () => {
  const { FROM, TO, AMOUNT_SOL, RPC_URL, PRIVATE_KEY, DRY_RUN } = process.env;
  const dryRun = DRY_RUN === '1';

  const { seed, pub } = parseSecret(PRIVATE_KEY);
  const fromPubkey = b58decode(FROM);
  const toPubkey = b58decode(TO);
  const systemProgram = Buffer.alloc(32); // all-zeros = System Program

  // Safety: the supplied key must actually control the --from address.
  if (b58encode(pub) !== FROM) {
    throw new Error(`private key does not match --from address.\n  key pubkey: ${b58encode(pub)}\n  --from:     ${FROM}`);
  }

  const lamports = BigInt(Math.round(parseFloat(AMOUNT_SOL) * 1e9));
  if (lamports <= 0n) throw new Error('amount must be > 0');

  // SystemProgram::Transfer instruction data: u32 index(2) + u64 lamports (LE).
  const data = Buffer.alloc(12);
  data.writeUInt32LE(2, 0);
  data.writeBigUInt64LE(lamports, 4);

  // Account keys ordered: writable-signer, writable-nonsigner, readonly-nonsigner.
  const keys = [fromPubkey, toPubkey, systemProgram];
  const header = Buffer.from([1, 0, 1]); // reqSigs=1, roSigned=0, roUnsigned=1

  // Recent blockhash (finalized) — the tx's "valid until" anchor.
  let blockhashB58;
  if (dryRun) {
    try {
      blockhashB58 = (await rpc(RPC_URL, 'getLatestBlockhash', [{ commitment: 'finalized' }])).value.blockhash;
    } catch (e) {
      // Dry run can proceed offline with a placeholder blockhash just to exercise signing.
      console.log('  (dry-run: could not fetch blockhash, using zero placeholder — ' + e.message + ')');
      blockhashB58 = b58encode(Buffer.alloc(32));
    }
  } else {
    blockhashB58 = (await rpc(RPC_URL, 'getLatestBlockhash', [{ commitment: 'finalized' }])).value.blockhash;
  }
  const recentBlockhash = b58decode(blockhashB58);

  // --- serialize the message ---
  const instr = Buffer.concat([
    Buffer.from([2]),                 // programIdIndex = system program (keys[2])
    encodeLength(2), Buffer.from([0, 1]), // accounts: from(0), to(1)
    encodeLength(data.length), data,
  ]);
  const message = Buffer.concat([
    header,
    encodeLength(keys.length), ...keys,
    recentBlockhash,
    encodeLength(1), instr,
  ]);

  // --- sign ---
  const signer = signerFromSeed(seed);
  const signature = crypto.sign(null, message, signer); // 64-byte ed25519 sig

  // Self-check the signature against the derived public key.
  const ok = crypto.verify(null, message, crypto.createPublicKey(signer), signature);
  if (!ok) throw new Error('internal error: signature failed self-verification');

  // --- assemble wire transaction: compact(sigs) + message ---
  const tx = Buffer.concat([encodeLength(1), signature, message]);
  const txBase64 = tx.toString('base64');

  console.log('  Lamports:        ' + lamports.toString());
  console.log('  Recent blockhash: ' + blockhashB58);
  console.log('  Tx signature:    ' + b58encode(signature));
  console.log('  Signed tx (b64): ' + txBase64);
  console.log('');

  if (dryRun) {
    console.log('  DRY RUN: transaction built and signature self-verified. NOT broadcast.');
    return;
  }

  const sig = await rpc(RPC_URL, 'sendTransaction', [txBase64, { encoding: 'base64' }]);
  console.log('  ✅ Broadcast. Signature: ' + sig);
  const cluster = process.env.CLUSTER || 'testnet';
  console.log('  Explorer: https://explorer.solana.com/tx/' + sig +
    (cluster === 'mainnet' ? '' : '?cluster=' + cluster));
})().catch((e) => { console.error('  ERROR: ' + e.message); process.exit(1); });
NODE