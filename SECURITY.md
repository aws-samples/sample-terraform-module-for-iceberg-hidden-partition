# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

We take the security of our project seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Please Do Not

- **Do not** open a public GitHub issue for security vulnerabilities
- **Do not** disclose the vulnerability publicly until it has been addressed
- **Do not** exploit the vulnerability beyond what is necessary to demonstrate it

### Please Do

1. **Email us directly** at: dl.altairhilltoppers@medtronic.com
2. **Include the following information**:
   - Type of vulnerability
   - Full paths of source file(s) related to the vulnerability
   - Location of the affected source code (tag/branch/commit or direct URL)
   - Step-by-step instructions to reproduce the issue
   - Proof-of-concept or exploit code (if possible)
   - Impact of the vulnerability, including how an attacker might exploit it

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Communication**: We will keep you informed about the progress of fixing the vulnerability
- **Timeline**: We aim to address critical vulnerabilities within 7 days
- **Credit**: We will credit you in the security advisory (unless you prefer to remain anonymous)

## Security Best Practices

When using this project, please follow these security best practices:

### IAM Permissions

- **Principle of Least Privilege**: Grant only the minimum permissions required
- **Scoped Resources**: Always scope IAM policies to specific resources
- **No Wildcards**: Avoid using wildcard actions (`*`) or resources (`*`)
- **Regular Audits**: Periodically review and audit IAM policies

### Credentials Management

- **Never Commit Secrets**: Do not commit AWS credentials, API keys, or secrets to the repository
- **Use IAM Roles**: Prefer IAM roles over long-term credentials
- **Rotate Credentials**: Regularly rotate access keys and credentials
- **Environment Variables**: Use environment variables or AWS Secrets Manager for sensitive data

### Terraform State

- **Remote State**: Store Terraform state in a secure remote backend (S3 with encryption)
- **State Locking**: Enable state locking to prevent concurrent modifications
- **Access Control**: Restrict access to Terraform state files
- **Encryption**: Enable encryption at rest for state files

### AWS Resources

- **Encryption**: Enable encryption for S3 buckets, Glue databases, and other resources
- **VPC**: Deploy resources in private subnets when possible
- **Security Groups**: Use restrictive security group rules
- **CloudTrail**: Enable CloudTrail for audit logging
- **GuardDuty**: Consider enabling AWS GuardDuty for threat detection

### Code Security

- **Dependency Scanning**: Regularly scan dependencies for vulnerabilities
- **Static Analysis**: Use tools like Checkov, Semgrep, and Bandit
- **Code Review**: Require code reviews for all changes
- **Automated Testing**: Implement automated security testing in CI/CD

## Security Scanning

This project uses the following security scanning tools:

### ASH (Automated Security Helper)

Run security scans before committing:

```bash
ash --source-dir . --output-dir ASH-Scan-results
```

### Terraform Security Tools

```bash
# Checkov
checkov -d .

# tfsec
tfsec .

# Terrascan
terrascan scan -t aws
```

### Python Security Tools

```bash
# Bandit
bandit -r Glue-DB-Module/scripts/

# Safety
safety check
```

## Known Security Considerations

### IAM Role Trust Relationships

- The Glue service role includes a trust relationship allowing the Admin role to assume it
- This is required for the partition management workflow
- An External ID (`terraform-iceberg-partition`) is used to prevent confused deputy attacks
- Review and adjust trust relationships based on your security requirements

### Local-Exec Provisioner

- The module uses `local-exec` provisioner to run Python scripts
- Scripts are executed with assumed role credentials
- Ensure the machine running Terraform is secure
- Consider using remote-exec or Lambda functions for production deployments

### Temporary Credentials

- The partition management script uses temporary credentials from STS AssumeRole
- Credentials are automatically cleaned up after script execution
- Credentials are not logged or persisted

## Compliance

This project follows security best practices for:

- AWS Well-Architected Framework (Security Pillar)
- CIS AWS Foundations Benchmark
- NIST Cybersecurity Framework

## Security Updates

Security updates will be released as:

- **Critical**: Immediate patch release
- **High**: Patch release within 7 days
- **Medium**: Included in next minor release
- **Low**: Included in next major release

## Security Advisories

Security advisories will be published:

- In the GitHub Security Advisories section
- In the CHANGELOG.md file
- Via email to registered users (if applicable)

## Additional Resources

- [AWS Security Best Practices](https://aws.amazon.com/security/best-practices/)
- [Terraform Security Best Practices](https://www.terraform.io/docs/cloud/guides/recommended-practices/index.html)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE Top 25](https://cwe.mitre.org/top25/)

## Contact

For security-related questions or concerns:

- **Email**: dl.altairhilltoppers@medtronic.com
- **Subject Line**: [SECURITY] Your subject here

Thank you for helping keep this project secure!
