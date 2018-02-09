#Start the authenticated client
import gdax

def authClient(sOrR, key, b64secret, passphrase):
    # Determine sandbox or real API (requires different set of API access credentials)
    if sOrR == 'r':
        apiurl = "https://api.gdax.com/"
    else:
        apiurl = "https://api-public.sandbox.gdax.com"

    auth_client = gdax.AuthenticatedClient(key, b64secret, passphrase,
                                  api_url=apiurl)

    #Algorithmic input
    
