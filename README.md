# x79d8

![build](https://github.com/quark-zju/x79d8/workflows/build/badge.svg)

Portable directory encryption.

Encrypted files are stored on the host OS filesystem. Decrypted files are
served via a local FTP service.

## Features

- Portable. Use FTP. Most systems support FTP.
- Dynamic sized. Space usage grows or shrinks based on usage.
- Industry encryption. AES256 for data encryption. scrypt for key derivation.

## Installation

```sh
cargo install x79d8
```

Note: Follow instructions from [the aesni crate](https://docs.rs/aesni) to
enable hardware acceleration. In short, you might need to set `RUSTFLAGS`
to `-C target-feature=+aes` during installation.

## Usage

### Initializing a new directory

Initialize x79d8 configs in an empty directory:

```
x79d8 init
```

Serve the directory. This will prompt for a new password:

```
x79d8 serve
```

Upload files to `ftp://127.0.0.2:2121`. Press Ctrl+C to store the encrypted
files on disk.

### Serving an existing directory

Serve the directory. This will prompt for the password and will only serve
the right content if the password is correct:

```
x79d8 serve
```

## Encryption

x79d8 uses AES256-CFB to encrypt blocks. A block has an integer `block_id`,
which is the file name on the host OS filesystem. The header of a block has a
random 128-bit integer `count`. The IV used for encrypting that block is
`blake2s(key, count, block_id)`. The `count` will be changed whenever the
block is written. If a block is deleted and re-added, its `count` will be
re-initialized by the OS rng. The OS RNG must be secure to eliminate IV reuse
in that case.

By default, a block is 1MB. Smaller files will be grouped into one block.
Larger files are will span across multiple blocks. This behavior can be changed
by the `--block-size-kb` option during `init`.

x79d8 uses scrypt to calculate the key from password. Its strength can be
changed by the `--scrypt-log-n` option during `init`.

x79d8 assumes it's a local service and there are no untrusted traffic. For
example, it does not use AEAD (authenticated encryption with associated data).
Do not expose x79d8 features to untrusted network! Do not allow untrusted
users to edit the encrypted files (in particular, they can replace a block
to its previous version to trick an IV-reuse case, if the OS RNG is also
insecure)!

## Durability

x79d8 starts to write changes to disk after 5 seconds. It uses WAL to ensure
data consistency.

## Background

I've been looking for TrueCrypt alternatives since its discontinuation. I'd
like the alternative to have dynamic space usage, is trustworthy (open-source,
audited or simple enough to audit), and cross-platform. Unfortunately, it's
not easy to find a good alternative. The RustCrypto libraries seem serious
enough to DIY a solution, and libunftp simplifies the "cross-platform" part
a lot.

x79d8 is only about 2k lines. The main encryption logic (`enc.rs`) is only
about 160 lines.
