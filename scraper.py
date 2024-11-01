import re
from urllib.parse import urlparse
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from collections import defaultdict
from utils import get_logger
from collections import Counter

visited_urls = set()
longest_page = {"url": "", "length": 0}
top_50 = defaultdict(int)
subdomains = defaultdict(int)

logger = get_logger("CRAWLER")


def scraper(url, resp):
    links = extract_next_links(url, resp)
    get_logs()
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    links = []

    # Check if the response status is 200 (OK)
    if resp.status != 200:
        logger.error(f"Failed to retrieve {url}: HTTP {resp.status}, error: {resp.error}")
        return links

    try:
        # Parse the page content with BeautifulSoup
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")

        # Find all 'a' tags with 'href' attributes (hyperlinks)
        for anchor in soup.find_all('a', href=True):
            # Get the href attribute (the link)
            href = anchor['href']

            # Join the relative URL with the base URL to form an absolute URL
            absolute_url = urljoin(url, href)

            # Add the absolute URL to the links list
            links.append(absolute_url)

    except Exception as e:
        logger.error(f"Error extracting links from {url}: {e}")

    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        # Remove fragment (#) from URL
        new_url = url.split("#")[0]

        if new_url in visited_urls or len(new_url) > 150:
            return False

        parsed = urlparse(new_url)

        # Only crawl HTTP/HTTPS URLs
        if parsed.scheme not in {"http", "https"}:
            return False

        # Filter out blacklisted queries
        query_blacklist = {"page_id", "share", "replytocom", "afg", "ical", "action"}
        if any(query in parsed.query for query in query_blacklist):
            return False

        # Allow only specific domains
        domain_whitelist = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu", "today.uci.edu/department/information_computer_sciences"]
        if not any(domain in parsed.netloc for domain in domain_whitelist) or "eecs.uci.edu" in parsed.netloc:
            return False

        # Filter out unwanted paths
        path_blacklist = {"/events/", "?filter", "/list/", "/day/", "/week/", "/month/"}
        if any(path in url.lower() for path in path_blacklist):
            return False

        if re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico"
                + r"|png|tiff?|mid|mp2|mp3|mp4"
                + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                + r"|epub|dll|cnf|tgz|sha1"
                + r"|thmx|mso|arff|rtf|jar|csv"
                + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False

        subdomains[parsed.netloc] += 1
        return True

    except TypeError as e:
        logger.error(f"TypeError for {url}: {e}")
        return False


def page_value(url, resp):
    """
    Evaluate the content of the response to determine if it's suitable for crawling.

    Parameters:
        url (str): The original URL of the page.
        resp: The response object containing the page content.

    Returns:
        bool: True if the page has sufficient meaningful content, False otherwise.
    """

    # Remove fragment (#) from URL
    new_url = url.split("#")[0]

    content = BeautifulSoup(resp.raw_response.content, "html.parser")
    textlist = content.text.split("\n")

    texts = {
        "i", "me", "my", "myself", "you", "your", "yours", "yourself", "yourselves",
        "he", "him", "his", "himself", "she", "her", "hers", "herself",
        "it", "its", "itself", "we", "us", "our", "ours", "ourselves",
        "they", "them", "their", "theirs", "themselves", "that", "these",
        "those", "what", "which", "who", "whom", "this", "that", "here",
        "there", "when", "where", "why", "how", "all", "any", "both",
        "few", "more", "most", "no", "nor", "not", "only", "some", "such",
        "into", "of", "off", "on", "for", "to", "with", "at", "by",
        "as", "from", "about", "before", "after", "during", "above",
        "below", "between", "up", "down", "over", "under", "again",
        "then", "but", "and", "or", "so", "if", "because", "until",
        "while", "having", "been", "am", "is", "are", "was", "were",
        "be", "do", "does", "did", "doing", "can", "could", "will",
        "would", "shall", "should", "may", "might", "must", "need",
        "want", "like", "just", "where", "too", "only", "very",
        "both", "each", "either", "neither", "this", "that", "whoever",
        "whatever", "whenever", "wherever", "whichever", "than", "somebody",
        "somewhere", "anybody", "anywhere", "nobody", "nowhere",
        "everything", "anything", "nothing", "something", "yet",
        "moreover", "furthermore", "besides", "also", "otherwise",
        "then", "therefore", "however", "nevertheless", "nonetheless",
        "whether", "although", "if", "unless", "despite", "although",
        "while", "whereas", "meanwhile", "finally", "initially",
        "subsequently", "ultimately", "similarly", "likewise",
        "consequently", "as", "like", "when", "since", "before",
        "after", "while", "while", "until", "provided", "as long as",
        "whereas", "although", "even though", "in spite of",
        "regardless of", "in case of", "given that", "in order to",
        "with respect to", "in terms of", "as far as", "in relation to",
        "pertaining to", "as it relates to", "related to", "further to",
        "by means of", "in light of", "owing to", "due to"
    }

    # Extract words and filter meaningful ones
    words = re.findall(r'\w+', ' '.join(textlist).lower())
    filtered_words = [word for word in words if len(word) >= 3 and word not in texts]

    # Skip pages with too few meaningful words
    if len(filtered_words) < 50:
        return False

    # Update word frequency
    word_counts = Counter(filtered_words)
    top_50.update(word_counts)

    # Update longest page data
    word_count = len(filtered_words)
    if longest_page["length"] < word_count:
        longest_page.update({"url": new_url, "length": word_count})

    return True


def get_logs():
    """Logs analytics information about the crawled pages."""
    # Ensure necessary variables are defined
    if 'visited_urls' not in locals() or 'longest_page' not in locals() or 'top_50' not in locals() or 'subdomains' not in locals():
        logger.error("One or more necessary variables are not defined.")
        return

    # 1. How many unique pages were found?
    unique_page_count = len(visited_urls)
    print(f"Unique pages found: {unique_page_count}")

    # 2. Longest page by word count
    longest_url = longest_page['url']
    longest_word_count = longest_page['length']
    print(f"Longest page: {longest_url} with {longest_word_count} words")

    # 3. Top 50 common words
    common_words = sorted(top_50.items(), key=lambda x: x[1], reverse=True)[:50]
    print("Top 50 common words (excluding stop words):")
    for word, count in common_words:
        print(f"{word}: {count}")

    # 4. Subdomains under uci.edu
    print("Subdomains under uci.edu (in alphabetical order):")
    uci_subdomains = {subdomain: count for subdomain, count in subdomains.items() if subdomain.endswith('.uci.edu')}
    for subdomain, count in sorted(uci_subdomains.items()):
        print(f"{subdomain}: {count}")

