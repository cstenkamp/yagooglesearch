# Standard Python libraries.
import logging
import os
import random
import time
import urllib
from datetime import datetime
import sys

# Third party Python libraries.
from bs4 import BeautifulSoup
import requests
import librecaptcha


__version__ = "2.0.0"

# Logging
ROOT_LOGGER = logging.getLogger("yagooglesearch")  # ISO 8601 datetime format by default.

# LOG_FORMATTER = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)s] %(message)s")
#
# Setup file logging.
# log_file_handler = logging.FileHandler("yagooglesearch.py.log")
# log_file_handler.setFormatter(LOG_FORMATTER)
# ROOT_LOGGER.addHandler(log_file_handler)
#
# Setup console logging.
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(LOG_FORMATTER)
# ROOT_LOGGER.addHandler(console_handler)

install_folder = os.path.abspath(os.path.split(__file__)[0])

try:
    user_agents_file = os.path.join(install_folder, "user_agents.txt")
    with open(user_agents_file, "r") as fh:
        USER_AGENTS_LIST = [_.strip() for _ in fh.readlines()]
except Exception:
    USER_AGENTS_LIST = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"]


# Load the list of result languages.  Compiled by viewing the source code at https://www.google.com/advanced_search for the supported languages.
try:
    result_languages_file = os.path.join(install_folder, "result_languages.txt")
    with open(result_languages_file, "r") as fh:
        RESULTS_LANGUAGES_DICT = {_.split("=")[0].strip():_.split("=")[1].strip() for _ in fh.readlines()}
except Exception as e:
    print(f"There was an issue loading the result languages file.  Exception: {e}")
    RESULTS_LANGUAGES_DICT = []


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

    # Used later to ensure there are not any URL parameter collisions.
    URL_PARAMETERS = ("btnG", "cr", "hl", "num", "q", "safe", "start", "tbs", "lr")

    def __init__(
        self,
        tld="com",
        lang_html_ui=None,
        lang_result=None,
        tbs="0",
        safe="off",
        country="",
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
        kill_event=None,
    ):
        """
        SearchClient
        :param str tld: Top level domain.
        :param str lang_html_ui: HTML User Interface language.
        :param str lang_result: Search result language.
        :param str tbs: Verbatim search or time limits (e.g., "qdr:h" => last hour, "qdr:d" => last 24 hours, "qdr:m"
            => last month).
        :param str safe: Safe search.
        :param str country: Country or region to focus the search on.  Similar to changing the TLD, but does not yield
            exactly the same results.  Only Google knows why...
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
        self.tld = tld
        self.lang_html_ui = lang_html_ui
        self.lang_result = lang_result.lower() if lang_result is not None else None
        self.tbs = tbs
        self.safe = safe
        self.country = country
        self.minimum_delay_between_paged_results_in_seconds = minimum_delay_between_paged_results_in_seconds
        self.default_user_agent = user_agent
        self.yagooglesearch_manages_http_429s = yagooglesearch_manages_http_429s
        self.http_429_cool_off_time_in_minutes = http_429_cool_off_time_in_minutes
        self.http_429_cool_off_factor = http_429_cool_off_factor
        self.proxy = proxy
        self.verify_ssl = verify_ssl
        self.verbosity = verbosity
        self.verbose_output = verbose_output
        self.google_exemption = google_exemption
        self.url_home = f"https://www.google.{self.tld}"
        self.kill_event = kill_event

        # Assign log level.
        ROOT_LOGGER.setLevel((6 - self.verbosity) * 10)

        # Argument checks.
        if self.lang_result is not None:
            if self.lang_result not in RESULTS_LANGUAGES_DICT:
                ROOT_LOGGER.error(
                    f"{self.lang_result} is not a valid language result.  See {result_languages_file} for the list of valid "
                    'languages.  Setting lang_result to "lang_en".'
                )
                self.lang_result = "lang_en"
            self.lang_result = RESULTS_LANGUAGES_DICT[self.lang_result]

        # Populate cookies with GOOGLE_ABUSE_EXEMPTION if it is provided.  Otherwise, initialize empty. Will be updated with each request in get_page().
        self.cookies = {"GOOGLE_ABUSE_EXEMPTION": self.google_exemption} if self.google_exemption else {}

        # Initialize proxy_dict.
        self.proxy_dict = {"http": self.proxy, "https": self.proxy} if self.proxy else {}

        # Suppress warning messages if verify_ssl is disabled.
        if not self.verify_ssl:
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

        self.reset_search(firsttime=True)
        ROOT_LOGGER.info("yagooglesearch initialized.")


    def reset_search(self, firsttime=False, new_ua=True):
        if new_ua:
            self.assign_user_agent(self.default_user_agent)
        if firsttime:
            # Simulates browsing to the https://www.google.com home page and retrieving the initial cookie.
            self.get_page(self.url_home)
            self.last_webcalls = self.webcalls = 1

    def get_url(self, query, start=None, num=None, extra_params=None):
        url = (
                self.url_home + f"/search?q={query}&safe={self.safe}&"
                + (f"hl={self.lang_html_ui}&" if self.lang_html_ui else "")
                + (f"lr={self.lang_result}&" if self.lang_result is not None else "")
                + (f"cr={self.country}&" if self.country is not None else "")
                + f"filter=0&tbs={self.tbs}"
        )
        url += (f"&btnG=Google+Search" if start in [None, 0] else f"&start={start}")
        if num is not None:
            url += f"&num={num}"

        extra_params = extra_params or {}
        for builtin_param in self.URL_PARAMETERS: # Check extra_params for overlapping parameters.
            if builtin_param in extra_params.keys():
                raise ValueError(f'GET parameter "{builtin_param}" is overlapping with the built-in GET parameter')
        for key, value in extra_params.items(): # Append extra GET parameters to the URL.  The keys and values are not URL encoded.
            url += f"&{key}={value}"

        return url

    def assign_user_agent(self, user_agent=None):
        self.user_agent = user_agent or random.choice(USER_AGENTS_LIST)

    @property
    def headers(self):
        return {"User-Agent": self.user_agent}

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

        new_http_429_cool_off_time_in_minutes = round(self.http_429_cool_off_time_in_minutes * self.http_429_cool_off_factor, 2)
        ROOT_LOGGER.info(
            f"Increasing HTTP 429 cool off time by a factor of {self.http_429_cool_off_factor}, "
            f"from {self.http_429_cool_off_time_in_minutes} minutes to {new_http_429_cool_off_time_in_minutes} minutes"
        )
        self.http_429_cool_off_time_in_minutes = new_http_429_cool_off_time_in_minutes

    def debug_requests_response(self, response):
        ROOT_LOGGER.debug(f"    status_code: {response.status_code}")
        ROOT_LOGGER.debug(f"    headers: {self.headers}")
        ROOT_LOGGER.debug(f"    cookies: {self.cookies}")
        ROOT_LOGGER.debug(f"    proxy: {self.proxy}")
        ROOT_LOGGER.debug(f"    verify_ssl: {self.verify_ssl}")

    def request(self, url, data=None, timeout=15, type="GET", additional_headers=None, update_cookies=True):
        ROOT_LOGGER.info(f"Requesting URL: {url}")
        headers = self.headers if additional_headers is None else {**self.headers, **additional_headers}

        if type == "POST":
            response = requests.post(url,
                                     data=data or {},
                                     proxies=self.proxy_dict,
                                     headers=headers,
                                     cookies=self.cookies,
                                     timeout=timeout,
                                     verify=self.verify_ssl)
        elif type == "GET":
            assert not data
            response = requests.get(url,
                                    proxies=self.proxy_dict,
                                    headers=headers,
                                    cookies=self.cookies,
                                    timeout=timeout,
                                    verify=self.verify_ssl)
        else:
            raise NotImplementedError()

        self.webcalls = getattr(self, "webcalls", 0) + 1  # we use this to check if we did actually access the web or just the cache
        self.debug_requests_response(response)
        if update_cookies:
            self.update_cookies(response.cookies)
            for history_elem in response.history:
                self.update_cookies(history_elem.cookies)
        return response

    def update_cookies(self, cookies):
        if not self.cookies:
            self.cookies = cookies
        else:
            self.cookies.update(cookies)

    def check_cookie_banner(self, response):
        # Click cookie-banner if it exists
        soup = BeautifulSoup(response.text, 'html.parser')
        if soup.find("form", {"action": "https://consent.google.de/save"}):
            ROOT_LOGGER.warning("Sending another request to get rid of the cookie-banner")
            cookie_forms = soup.find_all("form", {"action": f"https://consent.google.{self.tld}/save"})
            if cookie_forms:
                form_buttons = [[j.attrs["value"] for j in i.children if j.get("type") == "submit"] for i in cookie_forms]
                if any((accept := [i and i[0] in ["Accept all", "Alle akzeptieren"] for i in form_buttons])): # TODO other languages
                    accept_form = cookie_forms[accept.index(True)]
                    form_inputs = {i.attrs["name"]: i.attrs["value"]
                                   for i in accept_form.children
                                   if i.name == "input" and i.attrs["type"] == "hidden"}
                    response = self.request(accept_form.attrs["action"], data=form_inputs, type="POST")
        return response

    def set_consent_cookie(self, response):
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

    def solve_recaptcha(self, response, url):
        soup = BeautifulSoup(response.text, 'html.parser')
        sys.setrecursionlimit(3000)
        sitekey = soup.find("div", {"class": "g-recaptcha"}).attrs["data-sitekey"]
        token = librecaptcha.get_token(sitekey, url, self.user_agent, gui=False)
        sys.setrecursionlimit(1000)
        captcha_form = soup.find("form", {"id": "captcha-form"})
        form_inputs = {i.attrs["name"]: i.attrs["value"] for i in captcha_form.children if i.name == "input" and i.attrs["type"] == "hidden"}
        # del form_inputs["q"]
        form_inputs["g-recaptcha-response"] = token
        # https://developers.google.com/recaptcha/docs/verify: the response token is "a string argument to your callback function if data-callback is specified in either the g-recaptcha tag attribute"
        captcha_url = "https://www.google.com/sorry/index"
        referer = captcha_url + "?continue=" + "https:"+urllib.parse.quote(form_inputs["continue"][6:]) + f"&q={form_inputs['q']}"
        print("Referer:", referer)
        print("Form-Inputs:", form_inputs)
        response2 = self.request(captcha_url, data=form_inputs, additional_headers={"Referer": referer}) # TODO don't update cookies?
        print()

    def get_page(self, url):
        """
        Request the given URL and return the response page.
        :param str url: URL to retrieve.
        :rtype: str
        :return: Web page HTML retrieved for the given URL
        """
        response = self.request(url)

        # if this page displays a cookie-banner, follow the "accept" form and set the response-variable to the result of the following page instead
        response = self.check_cookie_banner(response)
        self.set_consent_cookie(response)
        # TODO: do these both ^ in the first get-google.com call

        html = ""

        if response.status_code == 200:
            html = response.text

        elif response.status_code == 429:
            ROOT_LOGGER.warning("Google is blocking your IP for making too many requests in a specific time period.")

            self.solve_recaptcha(response, url)
            # TODO: this ^ only optional and if interactive and ...

            # Calling script does not want yagooglesearch to handle HTTP 429 cool off and retry.  Just return a notification string.
            if not self.yagooglesearch_manages_http_429s:
                ROOT_LOGGER.info("Since yagooglesearch_manages_http_429s=False, yagooglesearch is done.")
                return "HTTP_429_DETECTED"

            ROOT_LOGGER.info(f"Sleeping for {self.http_429_cool_off_time_in_minutes} minutes...")
            time.sleep(self.http_429_cool_off_time_in_minutes * 60)
            self.http_429_detected()

            # Try making the request again.
            html = self.get_page(url)

        else:
            ROOT_LOGGER.warning(f"HTML response code: {response.status_code}")

        return html

    def results_from_url(self, url, prev_results_ref=None):
        # we want to cache this method, so ensure that this is side-effect free!
        results = []
        prev_results_ref = prev_results_ref or []

        html = self.get_page(url) # Request Google search results.

        if html == "HTTP_429_DETECTED":
            # HTTP 429 message returned from get_page() function, add "HTTP_429_DETECTED" to the set and return to the calling script.
            # this happens only if yagooglesearch_manages_429 == False
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
            if link not in prev_results_ref+results:
                link_rank = len(prev_results_ref)+len(results) # Approximate rank according to yagooglesearch.
                ROOT_LOGGER.info(f"Found unique URL #{link_rank+1}: {link}")
                elem = { "rank": link_rank, "title": title.strip(), "description": description.strip(), "url": link} \
                    if self.verbose_output else link
                results.append(elem)
            else:
                ROOT_LOGGER.info(f"Duplicate URL found: {link}")

        return results

    def sleep_against_429(self):
        if self.last_webcalls < self.webcalls: # we use this to check if something came from cache or not
            # Randomize sleep time between paged requests to make it look more human.
            random_sleep_time = random.choice(range(self.minimum_delay_between_paged_results_in_seconds, self.minimum_delay_between_paged_results_in_seconds + 11))
            ROOT_LOGGER.info(f"Sleeping {random_sleep_time} seconds until retrieving the next page of results...")
            for _ in range(random_sleep_time):
                if self.killed:
                    return
                time.sleep(1)
            self.last_webcalls = self.webcalls

    @property
    def killed(self):
        return self.kill_event is not None and self.kill_event.is_set()

    def search_gen(self, query, start=0, num=100, extra_params=None, max_result_urls=30, assign_new_ua=False):
        """
        :param str query: Query string.  Must NOT be url-encoded.
        :param int start: First page of results to retrieve.
        :param int num: Max number of results to pull back per page.  Capped at 100 by Google.
        :param int max_result_urls: Max URLs to return for the entire Google search.
        :param dict extra_params: A dictionary of extra HTTP GET parameters, which must be URL encoded.  For example if
            you don't want Google to filter similar results you can set the extra_params to {'filter': '0'} which will
            append '&filter=0' to every query.
        """
        if num > 100:
            ROOT_LOGGER.warning("The largest value allowed by Google for num is 100.  Setting num to 100.")
            num = 100
        query = urllib.parse.quote_plus(query)

        self.reset_search(new_ua=assign_new_ua) # if demanded, every  new search is done with a new user-agent
        search_result_list = [] # Consolidate search results.

        # Loop until we reach the maximum result results found or there are no more search results found to reach max_result_urls.
        while len(search_result_list) <= max_result_urls:

            ROOT_LOGGER.info(f"Stats: start={start}, num={num}, non-dup links found={len(search_result_list)}/{max_result_urls}")

            url = self.get_url(query, start=start, num=num, extra_params=extra_params)
            new_results = self.results_from_url(url, prev_results_ref=search_result_list)

            if new_results == ["HTTP_429_DETECTED"]:
                # this happens only if yagooglesearch_manages_429 == False
                search_result_list.append("HTTP_429_DETECTED")
                yield "HTTP_429_DETECTED" # TODO this is what effing exceptions are for
                return "HTTP_429_DETECTED"
            elif not new_results:
                # Determining if a "Next" URL page of results is not straightforward. If no valid links are found, the search results have been exhausted.
                ROOT_LOGGER.info("No valid search results found on this page. Returning.")
                return "SEARCH_EXHAUSTED"

            for elem in new_results:
                search_result_list.append(elem)
                yield elem
                if max_result_urls <= len(search_result_list):
                    # If we reached the limit of requested URLs, return with the results.
                    ROOT_LOGGER.info("returning because max_result_urls reached")
                    return "MAX_RESULTS_REACHED"

            start += num # Bump the starting page URL parameter for the next request.

            self.sleep_against_429()
            if self.killed:
                ROOT_LOGGER.info("returning because of kill-event")
                return "SEARCH_KILLED"

        ROOT_LOGGER.info("returning because at the end")