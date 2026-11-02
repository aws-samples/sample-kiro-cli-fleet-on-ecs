# Contributing

Thank you for your interest in contributing. Please read through this document before submitting any
issues or pull requests, so that we have all the necessary information to respond effectively.

## Reporting bugs and suggesting features

Use the issue tracker. Before filing, check existing and recently closed issues to see whether the
problem is already known. When reporting a bug, a useful report includes a reproducible test case, the
version in use, any modifications you have made relative to this repository, and anything unusual about
your environment or deployment.

## Contributing via pull request

Before sending a pull request, please confirm that you are working against the latest source on the
default branch, that you have checked existing open and recently merged pull requests for related work,
and that a significant amount of work is not being spent on something we may not want to merge. For a
larger change, open an issue to discuss it first.

To send us a pull request:

1. Fork the repository.
2. Modify the source, focusing on the specific change. Reformatting unrelated code makes a change hard
   to review.
3. Ensure the test suite passes locally with no AWS credentials configured.
4. Commit with a clear message describing the change and why it is being made.
5. Send us a pull request, answering any default questions in the pull request template.
6. Respond to review comments and to automated checks.

### Project conventions

This repository accompanies a published architecture pattern, so a few constraints are firmer than usual:

- **Dependencies are pinned and lockfiles are committed.** Reproducibility is a stated property of this
  sample. A change that unpins a dependency will be asked to repin it.
- **Tests run with no AWS account.** AWS API surfaces are faked. A test that requires deployed resources
  belongs in the benchmark harness, not the unit suite.
- **No account-specific values in source.** Account numbers, resource identifiers, private CIDR ranges,
  hostnames, and personal paths do not belong in this repository, including in comments, fixtures, and
  documentation examples. Parameterize instead. Run `./tools/scan-secrets.sh` before sending a change,
  or enable it as a pre-commit hook with `git config core.hooksPath tools`. A value committed once stays
  reachable in history even after a later commit removes it, so this is checked going in rather than
  cleaned up afterward.
- **Concurrency changes need concurrency tests.** The subscription pool exists to prevent overcommit
  under contention; a change to that path needs a test that actually contends.
- **Instrumentation ships with behavior.** If a change adds something the benchmark should measure, it
  adds the metric in the same change.

## Finding contributions to work on

Looking at existing issues is a good way to find something to work on, starting with any marked
`help wanted`.

## Code of conduct

This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.

## Security issue notifications

If you discover a potential security issue in this project, please notify AWS/Amazon Security via our
[vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do
**not** create a public GitHub issue.

## Licensing

See the [LICENSE](LICENSE) file for this project's licensing. We will ask you to confirm the licensing of
your contribution.

## Pushing to a CodeCommit mirror

If you keep a mirror in your own account, the SSO case needs the AWS credential helper — an
SSO identity cannot create the HTTPS Git credentials CodeCommit otherwise expects.

```bash
URL=https://git-codecommit.<region>.amazonaws.com/v1/repos/<repo>
git remote add backup "$URL"
# The empty value first resets the inherited helper list for this URL. Without it, a global
# helper such as macOS osxkeychain is tried first, offers a cached credential that has since
# expired, and the push fails with 403 while `aws sts get-caller-identity` still works.
git config --add credential."$URL".helper ""
git config --add credential."$URL".helper '!aws codecommit credential-helper $@'
git config credential."$URL".UseHttpPath true
```

Scoped to the one URL, so nothing about the machine's global git configuration changes.
