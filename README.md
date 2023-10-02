# ***EXPERIMENTAL BRANCH ONLY FOR TESTING, DO NOT USE IT***

This is an experimental branch to test some of my concerns about the Storj S3 backend. I want to check if public Storj S3 gateway have real "immediate data consistency" as required by s3ql even in hours of peak load. In order to test that storj s3 backend must be extended with this features:

- Added "consistency lock" logic, that will add dynamic delay after write/remove operations for objects with same keys, and block/warn about non-serialized read-write access to objects with same key (should not happen, ideally)
- TODO: Make delay to be ajustable in runtime
- TODO: Add extra background worker for checking consistency of recently modified objects, or after receiving some particular errors.
- TODO: Depending on current conditions ajust protective consistency intervals for "consistency lock" logic

# S3QL fork with improved Storj S3 backend support

This fork tries to solve 100K-object issue when using s3c backend to host your filesystem at Storj storage provider (storj.io) via public S3 gateway they provide.

See this issue for more info: <https://github.com/s3ql/s3ql/issues/326>

Fix currently implemented as separate storjs3 backend. In order to use it you must choose "storjs3://" prefix instead of "s3c://" prefix in your s3ql auth file

#### config example

```ini
[store]
storage-url: storjs3://gateway.storjshare.io/<your-bucket>/<optional-prefix-if-needed>/
backend-login: <put your storj s3 login credentials here>
backend-password: <put your storj s3 login credentials here>
backend-options: tcp-timeout=60
fs-passphrase: <file system password>
```

## IMPORTANT NOTES, read before use

- New `storjs3` backend is not interoperable with `s3c` backend. It changes object layout in your storj bucket that needed to fix the issue mentioned above. You can migrate your current filesystem manually, but you need some scripts for it that not published here. You can even convert your filesystem in broken state (after it suddenly reached 100K object limit and crashed after that), but you need to keep you previous s3c cache intact and also you need some other changes in s3ql source code to be able to run fsck. Contact me if you need help with this task.

- Currently there are no helper utilities or scripts available to convert `s3c` filesystem to `storj` layout and vise versa. Maybe I publish it someday, or maybe you can help me to implement it properly and post it here. Contact me if you are interested to help.

- This fork is not stable and it never become stable for critical production use without merging it into mainstream. Please, do not trust your precious data to it without setting up proper backups first. **YOU HAVE BEEN WARNED**.

- Initial motivation for me was to create quick solution that allow me to recover data from my storj bucket with s3ql filesystem, because it suddenly broke when reached 100K object limit. I cannot predict what other compatibility issues with Storj S3 API may arise in the future. These future problems may not be solvable at all.

- I will continue to use Storj to host my s3ql filesystem for some time, and I'll try to maintain this separate fork as long as it doesn't take up too much of my time. Merging new features from the upstream may be delayed. At some point it maybe impossible to merge new upstream revisions at all due to incompatible changes.

- If you are interested in long-term use of Storj to host your s3ql filesystem, please contact author via github issue above. Also consider helping implement a real long-term solution instead of this quick-fix.
