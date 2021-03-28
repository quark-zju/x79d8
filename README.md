# x79d8

![build](https://github.com/quark-zju/x79d8/workflows/build/badge.svg)

Portable directory encryption.

Encrypted files are stored on the host OS filesystem. Decrypted files are
served via a local FTP service.

## Features

- Portable. Use FTP. Most systems support FTP.
- Dynamic sized. Space usage grows or shrinks based on usage.
- Industry-standard encryption. AES256 for data encryption. scrypt for key derivation.

## Installation

```sh
cargo install x79d8
```

Note: Follow instructions from [the aesni crate](https://docs.rs/aesni) to
enable hardware acceleration. In short, you might need to set `RUSTFLAGS`
to `-C target-feature=+aes` during installation.

## Usage

Initialize x79d8 configs in an empty directory:

```
x79d8 init
```

Then serve the directory. This will prompt for a new password:

```
x79d8 serve
```

Upload files to `ftp://127.0.0.1:7968`. Press Ctrl+C to store the encrypted
files on disk. Press Ctrl+C to stop the FTP server.

To serve the directory again, run:

```
x79d8 serve
```

Enter the password set above to start the FTP server.

Setting `X79D8_LOG` to `debug` or `trace` enables debugging output.

## Encryption

x79d8 uses AES256-CFB to encrypt blocks. A block has an integer `block_id`,
which is the file name on the host OS filesystem. The header of a block has a
random 128-bit integer `count`. The IV used for encrypting that block is
`blake2s(key, count, block_id)`. The `count` will be changed whenever the
block is written. If a block is deleted and re-added, its `count` will be
re-initialized by the operating system random number generator. The OS RNG
must be secure to eliminate IV reuse in that case.

By default, a block is 1MB. Smaller files will be grouped into one block.
Larger files will span across multiple blocks. This behavior can be changed
by the `--block-size-kb` option during `init`.

x79d8 uses scrypt to calculate the key from password. Its strength can be
changed by the `--scrypt-log-n` option during `init`.

x79d8 assumes it's a local service and there is no untrusted traffic. For
example, it does not use AEAD (authenticated encryption with associated data).
Do not expose x79d8 features to untrusted network! Do not allow untrusted
users to edit the encrypted files (in particular, they can replace a block
to its previous version to trick an IV-reuse case if the OS RNG is also
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

----

# x79d8

跨平台的文件夹加密。

加密文件以普通文件的形式保存在系统中，解密后内容通过本地 FTP 服务访问。

## 特点

- 跨平台。使用主流系统都支持的 FTP，而不是如 fuse 等只在特定系统存在的功能。
- 动态空间占用。空间占用和实际使用有关，无需事先分配一大块分区。
- 业界标准加密。使用业界标准的 AES256 加密算法，和 scrypt 密钥扩充函数。

## 安装

首先[安装 Rust 编程语言](https://www.rust-lang.org/zh-CN/tools/install)，然后执行：

```sh
cargo install x79d8
```

注意：默认使用软件加密，对大文件会比较慢。若要使用 CPU 加密指令加速加密，请查看 [aesni](https://docs.rs/aesni) 文档。

## 使用方法

在空文件夹中执行以下命令进行初始化：

```sh
x79d8 init
```

然后使用：

```sh
x79d8 serve
```

来启动本地 FTP 服务，在 FTP 中添加要被加密的文件。按 Ctrl+C 终止 FTP 服务。

第一次启动 FTP 服务时设定密码，后续使用时需输入相同密码来解密。

设置 `X79D8_LOG` 环境变量为 `debug` 或 `trace` 可启用调试信息。

## 背景

在 TrueCrypt 不再更新后我想找一个替代品。替代品要能被信任（开源，代码简单我个人就能推敲），
功能上最好能不要预分配整个分区的大小，最好能跨平台。
遗憾的是，好像没有这样的替代品。RustCrypto 社区提供的加密库看起来质量都比较好，
libunftp 提供了方便的 FTP 支持，这样看来自己写一个也不是难事。

x79d8 只有约两千行，加密相关的部分（`enc.rs`）只有约 160 行，容易推敲。
