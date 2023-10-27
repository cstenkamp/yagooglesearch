# Standard Python libraries.
import logging
import os
import random
import time
import urllib
from datetime import datetime

# Third party Python libraries.
from bs4 import BeautifulSoup
import requests
import librecaptcha

# Custom Python libraries.

__version__ = "1.8.2"

# Logging
ROOT_LOGGER = logging.getLogger("yagooglesearch")
# ISO 8601 datetime format by default.
# LOG_FORMATTER = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)s] %(message)s")

# Setup file logging.
# log_file_handler = logging.FileHandler("yagooglesearch.py.log")
# log_file_handler.setFormatter(LOG_FORMATTER)
# ROOT_LOGGER.addHandler(log_file_handler)

# Setup console logging.
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(LOG_FORMATTER)
# ROOT_LOGGER.addHandler(console_handler)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"

# Load the list of valid user agents from the install folder.  The search order is:
# 1) user_agents.txt
# 2) default USER_AGENT
install_folder = os.path.abspath(os.path.split(__file__)[0])

try:
    user_agents_file = os.path.join(install_folder, "user_agents.txt")
    with open(user_agents_file, "r") as fh:
        user_agents_list = [_.strip() for _ in fh.readlines()]

except Exception:
    user_agents_list = [USER_AGENT]


# Load the list of result languages.  Compiled by viewing the source code at https://www.google.com/advanced_search for
# the supported languages.
try:
    result_languages_file = os.path.join(install_folder, "result_languages.txt")
    with open(result_languages_file, "r") as fh:
        result_languages_dict = {_.split("=")[0].strip():_.split("=")[1].strip()  for _ in fh.readlines()}
except Exception as e:
    print(f"There was an issue loading the result languages file.  Exception: {e}")
    result_languages_dict = []


def get_tbs(from_date, to_date):
    """Helper function to format the tbs parameter dates.  Note that verbatim mode also uses the &tbs= parameter, but
    this function is just for customized search periods.

    :param datetime.date from_date: Python date object, e.g. datetime.date(2021, 1, 1)
    :param datetime.date to_date: Python date object, e.g. datetime.date(2021, 6, 1)

    :rtype: str
    :return: Dates encoded in tbs format.
    """

    from_date = from_date.strftime("%m/%d/%Y")
    to_date = to_date.strftime("%m/%d/%Y")

    formatted_tbs = f"cdr:1,cd_min:{from_date},cd_max:{to_date}"

    return formatted_tbs


class SearchClient:
    def __init__(
        self,
        query,
        tld="com",
        lang_html_ui=None,
        lang_result=None,
        tbs="0",
        safe="off",
        start=0,
        num=100,
        country="",
        extra_params=None,
        max_search_result_urls_to_return=100,
        minimum_delay_between_paged_results_in_seconds=7,
        user_agent=None,
        yagooglesearch_manages_http_429s=True,
        http_429_cool_off_time_in_minutes=60,
        http_429_cool_off_factor=1.1,
        proxy="",
        verify_ssl=True,
        verbosity=5,
        verbose_output=False,
        google_exemption=None,
    ):
        """
        SearchClient
        :param str query: Query string.  Must NOT be url-encoded.
        :param str tld: Top level domain.
        :param str lang_html_ui: HTML User Interface language.
        :param str lang_result: Search result language.
        :param str tbs: Verbatim search or time limits (e.g., "qdr:h" => last hour, "qdr:d" => last 24 hours, "qdr:m"
            => last month).
        :param str safe: Safe search.
        :param int start: First page of results to retrieve.
        :param int num: Max number of results to pull back per page.  Capped at 100 by Google.
        :param str country: Country or region to focus the search on.  Similar to changing the TLD, but does not yield
            exactly the same results.  Only Google knows why...
        :param dict extra_params: A dictionary of extra HTTP GET parameters, which must be URL encoded.  For example if
            you don't want Google to filter similar results you can set the extra_params to {'filter': '0'} which will
            append '&filter=0' to every query.
        :param int max_search_result_urls_to_return: Max URLs to return for the entire Google search.
        :param int minimum_delay_between_paged_results_in_seconds: Minimum time to wait between HTTP requests for
            consecutive pages for the same search query.  The actual time will be a random value between this minimum
            value and value + 11 to make it look more human.
        :param str user_agent: Hard-coded user agent for the HTTP requests.
        :param bool yagooglesearch_manages_http_429s: Determines if yagooglesearch will handle HTTP 429 cool off and
           retries.  Disable if you want to manage HTTP 429 responses.
        :param int http_429_cool_off_time_in_minutes: Minutes to sleep if an HTTP 429 is detected.
        :param float http_429_cool_off_factor: Factor to multiply by http_429_cool_off_time_in_minutes for each HTTP 429
            detected.
        :param str proxy: HTTP(S) or SOCKS5 proxy to use.
        :param bool verify_ssl: Verify the SSL certificate to prevent traffic interception attacks.  Defaults to True.
            This may need to be disabled in some HTTPS proxy instances.
        :param int verbosity: Logging and console output verbosity.
        :param bool verbose_output: False (only URLs) or True (rank, title, description, and URL).  Defaults to False.
        :param str google_exemption: Google cookie exemption string.  This is a string that Google uses to allow certain
            google searches. Defaults to None.

        :rtype: List of str
        :return: List of URLs found or list of {"rank", "title", "description", "url"}
        """
        self.webcalls = 0
        self.query = urllib.parse.quote_plus(query)
        self.tld = tld
        self.lang_html_ui = lang_html_ui
        self.lang_result = lang_result.lower() if lang_result is not None else None
        self.tbs = tbs
        self.safe = safe
        self.start = start
        self.num = num
        self.country = country
        self.extra_params = extra_params or {}
        self.max_search_result_urls_to_return = max_search_result_urls_to_return
        self.minimum_delay_between_paged_results_in_seconds = minimum_delay_between_paged_results_in_seconds
        self.user_agent = user_agent
        self.yagooglesearch_manages_http_429s = yagooglesearch_manages_http_429s
        self.http_429_cool_off_time_in_minutes = http_429_cool_off_time_in_minutes
        self.http_429_cool_off_factor = http_429_cool_off_factor
        self.proxy = proxy
        self.verify_ssl = verify_ssl
        self.verbosity = verbosity
        self.verbose_output = verbose_output
        self.google_exemption = google_exemption
        self.url_home = f"https://www.google.{self.tld}"

        # Assign log level.
        ROOT_LOGGER.setLevel((6 - self.verbosity) * 10)

        # Argument checks.
        if self.lang_result is not None:
            if self.lang_result not in result_languages_dict:
                ROOT_LOGGER.error(
                    f"{self.lang_result} is not a valid language result.  See {result_languages_file} for the list of valid "
                    'languages.  Setting lang_result to "lang_en".'
                )
                self.lang_result = "lang_en"
            self.lang_result = result_languages_dict[self.lang_result]

        if self.num > 100:
            ROOT_LOGGER.warning("The largest value allowed by Google for num is 100.  Setting num to 100.")
            self.num = 100

        # Populate cookies with GOOGLE_ABUSE_EXEMPTION if it is provided.  Otherwise, initialize cookies to None.
        # It will be updated with each request in get_page().
        if self.google_exemption:
            self.cookies = {"GOOGLE_ABUSE_EXEMPTION": self.google_exemption}
        else:
            self.cookies = {}

        # Used later to ensure there are not any URL parameter collisions.
        self.url_parameters = ("btnG", "cr", "hl", "num", "q", "safe", "start", "tbs", "lr")

        # Default user agent, unless instructed by the user to change it.
        if not user_agent:
            self.user_agent = self.assign_random_user_agent()

        # Initialize proxy_dict.
        self.proxy_dict = {}

        # Update proxy_dict if a proxy is provided.
        if proxy:
            self.proxy_dict = {
                "http": self.proxy,
                "https": self.proxy,
            }

        # Suppress warning messages if verify_ssl is disabled.
        if not self.verify_ssl:
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


    def get_url(self, start=None, num=None):
        url = (
                self.url_home + f"/search?q={self.query}&safe={self.safe}&"
                + (f"hl={self.lang_html_ui}&" if self.lang_html_ui else "")
                + (f"lr={self.lang_result}&" if self.lang_result is not None else "")
                + (f"cr={self.country}&" if self.country is not None else "")
                + f"filter=0&tbs={self.tbs}"
        )
        url += (f"&btnG=Google+Search" if start in [None, 0] else f"&start={self.start}")
        if num is not None:
            url += f"&num={self.num}"

        for builtin_param in self.url_parameters: # Check extra_params for overlapping parameters.
            if builtin_param in self.extra_params.keys():
                raise ValueError(f'GET parameter "{builtin_param}" is overlapping with the built-in GET parameter')
        for key, value in self.extra_params.items(): # Append extra GET parameters to the URL.  The keys and values are not URL encoded.
            url += f"&{key}={value}"

        return url


    def assign_random_user_agent(self):
        """Assign a random user agent string.

        :rtype: str
        :return: Random user agent string.
        """

        random_user_agent = random.choice(user_agents_list)
        self.user_agent = random_user_agent

        return random_user_agent

    def filter_search_result_urls(self, link):
        """Filter links found in the Google result pages HTML code.  Valid results are absolute URLs not pointing to a
        Google domain, like images.google.com or googleusercontent.com.  Returns None if the link doesn't yield a valid
        result.

        :rtype: str
        :return: URL string
        """

        ROOT_LOGGER.debug(f"pre filter_search_result_urls() link: {link}")

        try:
            # Extract URL from parameter.  Once in a while the full "http://www.google.com/url?" exists instead of just
            # "/url?".  After a re-run, it disappears and "/url?" is present...might be a caching thing?
            if link.startswith("/url?") or link.startswith("http://www.google.com/url?"):
                urlparse_object = urllib.parse.urlparse(link, scheme="http")

                # The "q" key exists most of the time.
                try:
                    link = urllib.parse.parse_qs(urlparse_object.query)["q"][0]
                # Sometimes, only the "url" key does though.
                except KeyError:
                    link = urllib.parse.parse_qs(urlparse_object.query)["url"][0]

            # Create a urlparse object.
            urlparse_object = urllib.parse.urlparse(link, scheme="http")

            # Exclude urlparse objects without a netloc value.
            if not urlparse_object.netloc:
                ROOT_LOGGER.debug(
                    f"Excluding URL because it does not contain a urllib.parse.urlparse netloc value: {link}"
                )
                link = None

            # TODO: Generates false positives if specifying an actual Google site, e.g. "site:google.com fiber".
            if urlparse_object.netloc and ("google" in urlparse_object.netloc.lower()):
                ROOT_LOGGER.debug(f'Excluding URL because it contains "google": {link}')
                link = None

        except Exception:
            link = None

        ROOT_LOGGER.debug(f"post filter_search_result_urls() link: {link}")

        return link

    def http_429_detected(self):
        """Increase the HTTP 429 cool off period."""

        new_http_429_cool_off_time_in_minutes = round(
            self.http_429_cool_off_time_in_minutes * self.http_429_cool_off_factor, 2
        )
        ROOT_LOGGER.info(
            f"Increasing HTTP 429 cool off time by a factor of {self.http_429_cool_off_factor}, "
            f"from {self.http_429_cool_off_time_in_minutes} minutes to {new_http_429_cool_off_time_in_minutes} minutes"
        )
        self.http_429_cool_off_time_in_minutes = new_http_429_cool_off_time_in_minutes

    def get_page(self, url):
        """
        Request the given URL and return the response page.

        :param str url: URL to retrieve.

        :rtype: str
        :return: Web page HTML retrieved for the given URL
        """

        headers = {
            "User-Agent": self.user_agent,
        }

        ROOT_LOGGER.info(f"Requesting URL: {url}")
        self.webcalls += 1 # we use this to check if we did actually access the web or just the cache
        response = requests.get(
            url,
            proxies=self.proxy_dict,
            headers=headers,
            cookies=self.cookies,
            timeout=15,
            verify=self.verify_ssl,
        )

        # Update the cookies.
        if not self.cookies:
            self.cookies = response.cookies
        else:
            self.cookies.update(response.cookies)

        # Click cookie-banner if it exists
        soup = BeautifulSoup(response.text, 'html.parser')
        if soup.find("form", {"action": "https://consent.google.de/save"}):
            ROOT_LOGGER.warning("Sending another request to get rid of the cookie-banner")
            cookie_forms = soup.find_all("form", {"action": f"https://consent.google.{self.tld}/save"})
            if cookie_forms:
                form_buttons = [[j.attrs["value"] for j in i.children if j.get("type") == "submit"] for i in cookie_forms]
                if any((accept := [i and i[0] in ["Accept all", "Alle akzeptieren"] for i in form_buttons])):
                    accept_form = cookie_forms[accept.index(True)]
                    form_inputs = {i.attrs["name"]: i.attrs["value"]
                                   for i in accept_form.children
                                   if i.name == "input" and i.attrs["type"] == "hidden"}
                    response = requests.post(accept_form.attrs["action"],
                                             data=form_inputs,
                                             proxies=self.proxy_dict,
                                             headers=headers,
                                             cookies=self.cookies,
                                             timeout=15,
                                             verify=self.verify_ssl)

                    # Update the cookies.
                    if not self.cookies:
                        self.cookies = response.cookies
                    else:
                        self.cookies.update(response.cookies)
                        self.cookies.update(response.history[0].cookies)


        # Extract the HTTP response code.
        http_response_code = response.status_code

        # debug_requests_response(response)
        ROOT_LOGGER.debug(f"    status_code: {http_response_code}")
        ROOT_LOGGER.debug(f"    headers: {headers}")
        ROOT_LOGGER.debug(f"    cookies: {self.cookies}")
        ROOT_LOGGER.debug(f"    proxy: {self.proxy}")
        ROOT_LOGGER.debug(f"    verify_ssl: {self.verify_ssl}")

        # Google throws up a consent page for searches sourcing from a European Union country IP location.
        # See https://github.com/benbusby/whoogle-search/issues/311
        try:
            if response.cookies["CONSENT"].startswith("PENDING+"):
                ROOT_LOGGER.warning(
                    "Looks like your IP address is sourcing from a European Union location...your search results may "
                    "vary, but I'll try and work around this by updating the cookie."
                )

                # Convert the cookiejar data structure to a Python dict.
                cookie_dict = self.cookies if isinstance(self.cookies, dict) else requests.utils.dict_from_cookiejar(self.cookies)

                # Pull out the random number assigned to the response cookie.
                number = cookie_dict["CONSENT"].split("+")[1]

                # See https://github.com/benbusby/whoogle-search/pull/320/files
                """
                Attempting to dissect/breakdown the new cookie response values.

                YES - Accept consent
                shp - ?
                gws - "server:" header value returned from original request.  Maybe Google Workspace plus a build?
                fr - Original tests sourced from France.  Assuming this is the country code.  Country code was changed
                    to .de and it still worked.
                F - FX agrees to tracking. Modifying it to just F seems to consent with "no" to personalized stuff.
                    Not tested, solely based off of
                    https://github.com/benbusby/whoogle-search/issues/311#issuecomment-841065630
                XYZ - Random 3-digit number assigned to the first response cookie.
                """
                now = datetime.now()
                consent_cookie = 'YES+cb.{:d}{:02d}{:02d}-17-p0.de+F+{}'.format(now.year, now.month, now.day, number)
                # f"YES+shp.gws-20211108-0-RC1.fr+F+{number}"
                self.cookies["CONSENT"] = consent_cookie

                ROOT_LOGGER.debug(f"Updating cookie to: {self.cookies}")

        # "CONSENT" cookie does not exist.
        except KeyError:
            pass

        html = ""

        if http_response_code == 200:
            html = response.text

        elif http_response_code == 429:
            ROOT_LOGGER.warning("Google is blocking your IP for making too many requests in a specific time period.")
            import sys
            from book_to_money.scrap_book import browser_open
            from urllib import parse
            sys.setrecursionlimit(3000)
            sitekey = soup.find("div", {"class": "g-recaptcha"}).attrs["data-sitekey"]
            token = librecaptcha.get_token(sitekey, url, USER_AGENT, gui=False)
            sys.setrecursionlimit(1000)
            captcha_form = soup.find("form", {"id": "captcha-form"})
            form_inputs = {i.attrs["name"]: i.attrs["value"] for i in captcha_form.children if i.name == "input" and i.attrs["type"] == "hidden"}
            # del form_inputs["q"]
            form_inputs["g-recaptcha-response"] = token
            # https://developers.google.com/recaptcha/docs/verify: the response token is "a string argument to your callback function if data-callback is specified in either the g-recaptcha tag attribute"
            captcha_url = "https://www.google.com/sorry/index"
            referer = captcha_url + "?continue=" + "https:"+parse.quote(form_inputs["continue"][6:]) + f"&q={form_inputs['q']}"
            print("Referer:", referer)
            print("Form-Inputs:", form_inputs)
            response2 = requests.post(captcha_url, data=form_inputs, proxies=self.proxy_dict, headers={**headers, "Referer": referer}, cookies=self.cookies, timeout=15, verify=self.verify_ssl)
            print()


            # Calling script does not want yagooglesearch to handle HTTP 429 cool off and retry.  Just return a
            # notification string.
            if not self.yagooglesearch_manages_http_429s:
                ROOT_LOGGER.info("Since yagooglesearch_manages_http_429s=False, yagooglesearch is done.")
                return "HTTP_429_DETECTED"

            ROOT_LOGGER.info(f"Sleeping for {self.http_429_cool_off_time_in_minutes} minutes...")
            time.sleep(self.http_429_cool_off_time_in_minutes * 60)
            self.http_429_detected()

            # Try making the request again.
            html = self.get_page(url)

        else:
            ROOT_LOGGER.warning(f"HTML response code: {http_response_code}")

        return html


    def results_from_url(self, url):
        # we want to cache this method, so ensure that this is side-effect free!
        results = []

        html = self.get_page(url) # Request Google search results.
        # HTTP 429 message returned from get_page() function, add "HTTP_429_DETECTED" to the set and return to the calling script.
        if html == "HTTP_429_DETECTED":
            return ["HTTP_429_DETECTED"]
        soup = BeautifulSoup(html, "html.parser")

        # Find all HTML <a> elements.
        try:
            anchors = soup.find(id="search").find_all("a")
        except AttributeError:
            # Sometimes (depending on the User-Agent) there is no id "search" in html response.
            gbar = soup.find(id="gbar")
            if gbar:
                gbar.clear() # Remove links from the top bar.
            anchors = soup.find_all("a")

        for a in anchors:
            try:
                link = a["href"]
            except KeyError:
                ROOT_LOGGER.warning(f"No href for link: {a}")
                continue

            link = self.filter_search_result_urls(link) # Filter invalid links and links pointing to Google itself.
            if not link:
                continue

            if self.verbose_output:
                try:
                    title = a.get_text() # Extract the URL title.
                except Exception:
                    ROOT_LOGGER.warning(f"No title for link: {link}")
                    title = ""

                try:  # Extract the URL description.
                    description = a.parent.parent.contents[1].get_text()
                    if description == "": # Sometimes Google returns different structures.
                        description = a.parent.parent.contents[2].get_text()
                except Exception:
                    ROOT_LOGGER.warning(f"No description for link: {link}")
                    description = ""

            # Check if URL has already been found.
            if link not in self.search_result_list+results:
                ROOT_LOGGER.info(f"Found unique URL #{len(self.search_result_list)+len(results)+1}: {link}")
                elem = { "rank": len(self.search_result_list)+len(results),  # Approximate rank according to yagooglesearch.
                         "title": title.strip(),  # Remove leading and trailing spaces.
                         "description": description.strip(),  # Remove leading and trailing spaces.
                         "url": link,
                       } if self.verbose_output else link
                results.append(elem)
            else:
                ROOT_LOGGER.info(f"Duplicate URL found: {link}")

        return results

    def search_gen(self, kill_event=None):
        self.search_result_list = [] # Consolidate search results.

        html = self.get_page(self.url_home)  # Simulates browsing to the https://www.google.com home page and retrieving the initial cookie.
        self.last_webcalls = self.webcalls

        # Loop until we reach the maximum result results found or there are no more search results found to reach
        # max_search_result_urls_to_return.
        while len(self.search_result_list) <= self.max_search_result_urls_to_return:

            ROOT_LOGGER.info(
                f"Stats: start={self.start}, num={self.num}, total_valid_links_found={len(self.search_result_list)} / "
                f"max_search_result_urls_to_return={self.max_search_result_urls_to_return}"
            )

            url = self.get_url(self.start, self.num)
            new_results = self.results_from_url(url)

            if new_results == ["HTTP_429_DETECTED"]:
                self.search_result_list.append("HTTP_429_DETECTED")
                yield "HTTP_429_DETECTED" # TODO this is what effing exceptions are for
            elif not new_results:
                # Determining if a "Next" URL page of results is not straightforward. If no valid links are found, the search results have been exhausted.
                ROOT_LOGGER.info("No valid search results found on this page. Returning.")
                return

            for elem in new_results:
                self.search_result_list.append(elem)
                yield elem
                if self.max_search_result_urls_to_return <= len(self.search_result_list):
                    # If we reached the limit of requested URLs, return with the results.
                    ROOT_LOGGER.info("returning because self.max_search_result_urls_to_return reached")
                    return

            self.start += self.num # Bump the starting page URL parameter for the next request.

            if self.last_webcalls < self.webcalls: # we use this to check if something came from cache or not
                # Randomize sleep time between paged requests to make it look more human.
                random_sleep_time = random.choice(range(self.minimum_delay_between_paged_results_in_seconds, self.minimum_delay_between_paged_results_in_seconds + 11))
                ROOT_LOGGER.info(f"Sleeping {random_sleep_time} seconds until retrieving the next page of results...")
                for _ in range(random_sleep_time):
                    if kill_event is not None and kill_event.is_set():
                        ROOT_LOGGER.info("returning because of kill-event")
                        return
                    time.sleep(1)
                self.last_webcalls = self.webcalls

        ROOT_LOGGER.info("returning because at the end")