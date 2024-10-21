def read_credentials(fullpath):
    """Read the credentials file and return the username and password
    
    Args:
    fullpath (str) : full path to the credentials file
    
    Returns:
    dict : dictionary with keys 'username' and 'password'
    """
    credentials ={}
    with open(fullpath) as f:
        lines = f.readlines()
        for line in lines:
            key,value = line.strip().split('=')
            credentials[key] = value
    return credentials