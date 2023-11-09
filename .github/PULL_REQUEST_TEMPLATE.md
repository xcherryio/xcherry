## Why make this pull request?

[Explain why you are making this pull request and what problem it solves.]

## What has changed

[Summarize what components of the repo is updated]

[Link to xdb-apis/xdb-golang-sdk PRs if it's on top of any API changes]

- API change link: ...
- Golang SDK change link: ...
- Server Component 1: ...
- Server Component 2: ...

## How to test this pull request?

[If writing Integration test in Golang SDK repo, please provide link to the pull request of Golang SDK Repo]

[It's recommended to write integration test in Golang SDK repo, and enabled in this server repo first, 
without enabling in the SDK repo. After this PR is merged, enable and merge the integration test in the SDK repo]

[Alternatively if Java/other SDK repo is preferred, then just test locally against server PR. 
After the server PR is merged, merge the integration test in the SDK repo]

## Checklist before merge
[ ] If applicable, merge the xdb-apis/xdb-golang-sdk PRs to main branch
[ ] If applicable, merge the xdb-apis/xdb-apis PRs to main branch
[ ] Update `go.mod` to use the commitID of the main branches for xdb-apis/xdb-golang-sdk
