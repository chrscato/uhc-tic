"""Common HTTP headers for making requests."""

from typing import Dict

def get_cloudfront_headers(url: str = None) -> Dict[str, str]:
    """Get standard headers for CloudFront requests to avoid bot detection.
    
    Args:
        url: Optional URL to customize headers for specific domains
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, application/octet-stream, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',  # Changed to same-origin for CloudFront
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }
    
    # Add domain-specific headers
    if url:
        # Handle Cigna CloudFront domain
        if 'd25kgz5rikkq4n.cloudfront.net' in url:
            domain = 'https://d25kgz5rikkq4n.cloudfront.net'
            headers.update({
                'Origin': domain,
                'Referer': f"{domain}/",
            })
    
    return headers