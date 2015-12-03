# Longevity-Stress-Performance Tests for HAWQ


## Git LFS (Large File Storage)


### Overview

```
Git Large File Storage (LFS) replaces large files such as audio samples, videos, datasets, and graphics with text pointers inside Git, while storing the file contents on a remote server like GitHub.com or GitHub Enterprise.
```
More info [here]((https://git-lfs.github.com/).


### Prerequisites

It is required that you have a working installation of Git on your machine. Installation instructions for Git can be found [here](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).


### Installation

You can install Git LFS in several different ways, depending on your setup and preferences.

- Linux users can install Debian or RPM packages from [PackageCloud](https://packagecloud.io/github/git-lfs).
- Mac users can install from [Homebrew](https://github.com/Homebrew/homebrew) with `brew install git-lfs`.
- [Binary packages](https://github.com/github/git-lfs/releases) are also available for Windows, Mac, Linux, and FreeBSD.

More info [here](https://github.com/github/git-lfs/#getting-started).


### Usage

To view or control which files in your repository that LFS manages, use the `git lfs track` command.  For help, run `git lfs help track`. Git stores information about which files LFS should track in `.gitattributes`.  LFS can track in three ways:

  - Single files: `git lfs track lfs.py`
  - All files matching a pattern: `git lfs track *.ans`
  - All files under a directory: `git lfs track workloads/TPCH/queries_ans/`

To view which files LFS is currently managing, run `git lfs ls-files`.  Note that this may not be up to date with respect to what `git lfs track` or manual inspection of `.gitattributes` shows.  By design, LFS doesn't start or stop tracking a file when `git lfs (un)track` specifies it.  LFS will only begin to (un)track the file locally once that file has been referenced in a local commit.  GitHub will begin tracking the file with LFS once that commit has been pushed.

In other words, if you want to immediately begin tracking `unicorn.py` with LFS, you have to do this:

1. Tell LFS to track `unicorn.py` the next time a commit associated with that file is pushed:
    * `git lfs track unicorn.py`
1. Ensure that `unicorn.py` is now tracked:
    * `git lfs track` includes `unicorn.py`
    * `.gitattributes` has been modified
    * Note that `unicorn.py` won't show up in `git lfs ls-files` yet, and the local copy won't have been replaced with its corresponding LFS representation.
1. Force LFS to begin tracking `unicorn.py` immediately:
    * `git add .gitattributes`
    * *Optional: Commit changes to `.gitattributes`*
    * Modify `unicorn.py` in some trivial way
    * `git add unicorn.py`
    * Commit.  `git lfs ls-files` now lists `unicorn.py`, but LFS is not yet tracking `unicorn.py` on the remote server.
    * Push.  On GitHub, selecting `unicorn.py` displays LFS information.