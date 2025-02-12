# btree-ondisk

A Rust implementation of BTree structure on persistent storage in userspace.

Codebase is inspired by and partially derived from [NILFS2](https://docs.kernel.org/filesystems/nilfs2.html).

NILFS2 is a log-structured file system implementation for the Linux kernel.

**NOTICE**: This library itself does not include persistent part, user should implement persistent process on top of this library.

## Under Developement

:warning: This library is currently under developement and is **NOT** recommended for production.

## Examples

See [examples](examples/) for how to use.

## Credits

In loving memory of my father, Mr. Dai Wenhua, Who bought me my first computer.

## License

This library is licensed under the GPLv2 or later License. See the [LICENSE](LICENSE) file.
