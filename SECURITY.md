# Security

## Reporting a vulnerability

Do not open a public issue containing a token, password, credential-bearing URL, health payload,
or sensitive state report. Contact the repository maintainers through a private channel and share
only the information required to reproduce the issue.

## Sensitive data

The repository must never contain:

- `.env`;
- DHIS2 personal access tokens or passwords;
- CDS or EDH keys;
- exported health data values;
- `state/`, `cache/`, `reports/`, or `data/` directories;
- logs containing authentication headers.

If a secret is published accidentally, revoke it immediately. Removing only the latest commit is
not enough: treat the secret as compromised and clean the Git history according to your
organization's incident procedure.

## Deployment

- use a dedicated service account with least privilege;
- restrict `.env` permissions to `0600`;
- protect the DHIS2 origin with a firewall or reverse proxy;
- prefer a private API route restricted to the import server when a public proxy limits long
  requests;
- back up and protect checkpoints and audit reports.
