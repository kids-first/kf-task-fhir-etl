
def _set_authorization(genomic_file):
    """
    Create the auth codes for the security label from genomic_file authz or acl field

    - authz takes precedence over acl
    - Ensure all values are in "old" acl format
    """
    authz = genomic_file.get("authz")
    if authz:
        new_codes = []
        for code in authz:
            new_code = code.split("/")[-1].strip()
            if new_code == "open":
                new_code = "*"
            new_codes.append(new_code)
    else:
        new_codes = genomic_file.get("acl") or []

    return new_codes
    

