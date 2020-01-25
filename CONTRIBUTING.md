# Contributing

Thanks for considering contributing! The group of maintainers for this project is small, and so we cannot reach our goals without you.

## How can I contribute?

### Did you find a bug?

If you've found a bug, please first search through the [existing issues](https://github.com/rusty-celery/rusty-celery/issues) to see if it has already been reported. If it was already reported and you have additional information that may be helpful, please include that information in a comment on the issue. Otherwise please submit a new issue with a descriptive title and as much context as you can.

### Did you write a solution to a bug?

If you've found a solution to a bug that you or someone else has already reported, please make sure there is not already a pull request open for the same fix. If not, please submit a PR and link to the original issue.

For details on how to submit a PR, please see the section below on [contributing code](#contributing-code).

### Do you have a suggestion for an enhancement?

We use GitHub issues to track feature requests. Before submitting a feature request, please search to see if something similar has already been suggested. When creating a feature request, provide a clear title and description outlining why the enhancement would be useful. If the enhancement would involve changes to the API, please include code examples to demonstrate how the enhancement would be used.

### Would you like to help implement a missing feature?

If you're looking for something you can help with, search for any open issues with the label ["Status: Help Wanted"](https://github.com/rusty-celery/rusty-celery/issues?q=is%3Aopen+is%3Aissue+label%3A%22Status%3A+Help+Wanted%22). If you find one that suits you, please comment on the issue with your intent to work on it and any questions you might have.

When you're prepared to start working on it, please read the section below on [contributing code](#contributing-code).

## Contributing code

### First time contributions

Before contributing code you will need to have the Rust toolchain installed. This is usually a painless procedure that can be accomplished with a single line of code:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

For information see the official [install guide](https://www.rust-lang.org/tools/install).

The next step is to fork the repository on GitHub and then clone your fork locally:

```bash
git clone https://github.com/USERNAME/rusty-celery.git
```

or 

```bash
git clone git@github.com:USERNAME/rusty-celery.git
```

if you want to clone using SSH.

Then set the `upstream` remote to the original repo. This is essential if you plan on contributing regularly, as it let's you easily pull new commits from the original repo into your fork.

```
git remote add upstream https://github.com/rusty-celery/rusty-celery.git
```

### Building and testing

To compile the codebase just type `make` (the Makefile in this repo is mostly just a wrapper for common `cargo` commands). To lint the code use `make lint`, and to run tests use `make test`. Before submitting a PR please run all of these commands and make sure they complete successfully.

Additionally, if your contribution involves changes to the API documentation, please build and proofread the docs with `cargo doc --workspace --open`.

### Continuing to contribute

When you are preparing to make additional contributions you should first make sure your local fork is up-to-date:

```bash
git checkout master  # if not already on master
git pull --rebase upstream master
git push
```

Now if you visit your fork of the project on GitHub, it should say that your master branch is up-to-date with the original master branch.

Next create a new branch to work on your fix/improvement/feature:

```bash
git checkout -b BRANCH
git push --set-upstream origin BRANCH
```
