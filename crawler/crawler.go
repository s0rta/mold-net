package crawler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"lieu/types"
	"lieu/util"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/queue"
)

// the following domains are excluded from crawling & indexing, typically because they have a lot of microblog pages
// (very spammy)
func getBannedDomains(path string) []string {
	return util.ReadList(path, "\n")
}

func getBannedSuffixes(path string) []string {
	return util.ReadList(path, "\n")
}

func getBoringWords(path string) []string {
	return util.ReadList(path, "\n")
}

func getBoringDomains(path string) []string {
	return util.ReadList(path, "\n")
}

func getAboutHeuristics(path string) []string {
	return util.ReadList(path, "\n")
}

func getPreviewQueries(path string) []string {
	previewQueries := util.ReadList(path, "\n")
	if len(previewQueries) > 0 {
		return previewQueries
	} else {
		return []string{"main p", "article p", "section p", "p"}
	}
}

func find(list []string, query string) bool {
	for _, item := range list {
		if item == query {
			return true
		}
	}
	return false
}

func getLink(target string) string {
	// remove anchor links
	if strings.Contains(target, "#") {
		target = strings.Split(target, "#")[0]
	}
	if strings.Contains(target, "?") {
		target = strings.Split(target, "?")[0]
	}
	target = strings.TrimSpace(target)
	// remove trailing /
	return strings.TrimSuffix(target, "/")
}

func getWebringLinks(path string) ([]string, []int) {
	var links []string
	var depths []int

	candidates := util.ReadList(path, "\n")
	for _, l := range candidates {
		v := strings.Split(l, " ")
		u, err := url.Parse(v[0])
		if err != nil {
			continue
		}
		if u.Scheme == "" {
			u.Scheme = "https"
		}
		links = append(links, u.String())

		depth, err := strconv.Atoi(v[1])
		if err != nil {
			panic(err)
		}
		depths = append(depths, depth)

	}
	return links, depths
}

func getDomains(links []string) ([]string, []string) {
	var domains []string
	// sites which should have stricter crawling enforced (e.g. applicable for shared sites like tilde sites)
	// pathsites are sites that are passed in which contain path,
	// e.g. https://example.com/site/lupin -> only children pages of /site/lupin/ will be crawled
	var pathsites []string
	for _, l := range links {
		u, err := url.Parse(l)
		if err != nil {
			continue
		}
		domains = append(domains, u.Hostname())
		if len(u.Path) > 0 && (u.Path != "/" || u.Path != "index.html") {
			pathsites = append(pathsites, l)
		}
	}
	return domains, pathsites
}

func findSuffix(suffixes []string, query string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(strings.ToLower(query), suffix) {
			return true
		}
	}
	return false
}

func cleanText(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	whitespace := regexp.MustCompile(`\p{Z}+`)
	s = whitespace.ReplaceAllString(s, " ")
	return s
}

func handleIndexing(c *colly.Collector, previewQueries []string, heuristics []string, linkDepths map[string]int) {
	c.OnHTML("meta[name=\"keywords\"]", func(e *colly.HTMLElement) {
		fmt.Println("keywords", cleanText(e.Attr("content")), e.Request.URL, linkDepths[e.Request.URL.String()])
	})

	c.OnHTML("meta[name=\"description\"]", func(e *colly.HTMLElement) {
		desc := cleanText(e.Attr("content"))
		if len(desc) > 0 && len(desc) < 1500 {
			fmt.Println("desc", desc, e.Request.URL, linkDepths[e.Request.URL.String()])
		}
	})

	c.OnHTML("meta[property=\"og:description\"]", func(e *colly.HTMLElement) {
		ogDesc := cleanText(e.Attr("content"))
		if len(ogDesc) > 0 && len(ogDesc) < 1500 {
			fmt.Println("og-desc", ogDesc, e.Request.URL, linkDepths[e.Request.URL.String()])
		}
	})

	c.OnHTML("html[lang]", func(e *colly.HTMLElement) {
		lang := cleanText(e.Attr("lang"))
		if len(lang) > 0 && len(lang) < 100 {
			fmt.Println("lang", lang, e.Request.URL, linkDepths[e.Request.URL.String()])
		}
	})

	// get page title
	c.OnHTML("title", func(e *colly.HTMLElement) {
		fmt.Println("title", cleanText(e.Text), e.Request.URL, linkDepths[e.Request.URL.String()])
	})

	c.OnHTML("body", func(e *colly.HTMLElement) {
	QueryLoop:
		for i := 0; i < len(previewQueries); i++ {
			// After the fourth paragraph we're probably too far in to get something interesting for a preview
			elements := e.DOM.Find(previewQueries[i])
			for j := 0; j < 4 && j < elements.Length(); j++ {
				element_text := elements.Slice(j, j+1).Text()
				paragraph := cleanText(element_text)
				if len(paragraph) < 1500 && len(paragraph) > 20 {
					if !util.Contains(heuristics, strings.ToLower(paragraph)) {
						fmt.Println("para", paragraph, e.Request.URL, linkDepths[e.Request.URL.String()])
						break QueryLoop
					}
				}
			}
		}
		paragraph := cleanText(e.DOM.Find("p").First().Text())
		if len(paragraph) < 1500 && len(paragraph) > 0 {
			fmt.Println("para-just-p", paragraph, e.Request.URL, linkDepths[e.Request.URL.String()])
		}

		// get all relevant page headings
		collectHeadingText("h1", e, linkDepths)
		collectHeadingText("h2", e, linkDepths)
		collectHeadingText("h3", e, linkDepths)
	})
}

func collectHeadingText(heading string, e *colly.HTMLElement, linkDepths map[string]int) {
	for _, headingText := range e.ChildTexts(heading) {
		if len(headingText) < 500 {
			fmt.Println(heading, cleanText(headingText), e.Request.URL, linkDepths[e.Request.URL.String()])
		}
	}
}

func SetupDefaultProxy(config types.Config) error {
	// no proxy configured, go back
	if config.General.Proxy == "" {
		return nil
	}
	proxyURL, err := url.Parse(config.General.Proxy)
	if err != nil {
		return err
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
	}

	http.DefaultClient = httpClient
	return nil
}

func Precrawl(config types.Config) {
	myClient := &http.Client{Timeout: 10 * time.Second}
	// setup proxy
	err := SetupDefaultProxy(config)
	if err != nil {
		log.Fatal(err)
	}

	res, err := myClient.Get(config.General.URL)

	if err != nil {
		log.Fatal(err)
	}

	depthCounter := 0
	checked := mapset.NewSet()
	allHyphae := mapset.NewSet()
	allSites := mapset.NewSet()

	precrawled := false

	for !precrawled {
		body, err := ioutil.ReadAll(res.Body)

		if err != nil {
			log.Fatal(err)
		}

		var cluster types.Cluster
		json.Unmarshal(body, &cluster)
		cluster.Depth = depthCounter

		defer res.Body.Close()

		checked.Add(types.Hypha{Url: cluster.Location, Depth: cluster.Depth})

		for _, v := range cluster.Hyphae {
			allHyphae.Add(types.Hypha{Url: v, Depth: cluster.Depth + 1})
		}

		for _, v := range cluster.Spores {
			allSites.Add(types.Site{Url: v, Depth: cluster.Depth})
		}

		diff := allHyphae.Difference(checked)

		if diff.Cardinality() == 0 {
			precrawled = true
		} else {
			nextToExplore := diff.Pop()
			d := nextToExplore.(types.Hypha)
			depthCounter = d.Depth
			res, err = myClient.Get(d.Url)

			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if err != nil {
		log.Fatal(err)
	}

	util.Check(err)

	if res.StatusCode != 200 {
		log.Fatal("status not 200")
	}

	BANNED := getBannedDomains(config.Crawler.BannedDomains)
	for _, item := range allSites.ToSlice() {
		h := item.(types.Site)
		link := getLink(fmt.Sprintf("%v", h.Url))
		u, err := url.Parse(link)
		// invalid link
		if err != nil {
			continue
		}
		domain := u.Hostname()
		if find(BANNED, domain) {
			continue
		}
		fmt.Println(link, h.Depth)
	}
}

func Crawl(config types.Config) {
	// setup proxy
	err := SetupDefaultProxy(config)
	if err != nil {
		log.Fatal(err)
	}
	SUFFIXES := getBannedSuffixes(config.Crawler.BannedSuffixes)
	links, depths := getWebringLinks(config.Crawler.Webring)
	domains, pathsites := getDomains(links)
	initialDomain := config.General.URL

	linkDepths := make(map[string]int)

	// TODO: introduce c2 for scraping links (with depth 1) linked to from webring domains
	// instantiate default collector
	c := colly.NewCollector(
		colly.MaxDepth(3),
	)
	if config.General.Proxy != "" {
		c.SetProxy(config.General.Proxy)
	}

	q, _ := queue.New(
		5, /* threads */
		&queue.InMemoryQueueStorage{MaxSize: 100000},
	)

	for i, link := range links {
		q.AddURL(link)
		linkDepths[link] = depths[i]
	}

	c.UserAgent = "moldy"
	c.AllowedDomains = domains
	c.AllowURLRevisit = false
	c.DisallowedDomains = getBannedDomains(config.Crawler.BannedDomains)

	delay, _ := time.ParseDuration("1000ms")
	c.Limit(&colly.LimitRule{DomainGlob: "*", Delay: delay, Parallelism: 3})

	boringDomains := getBoringDomains(config.Crawler.BoringDomains)
	boringWords := getBoringWords(config.Crawler.BoringWords)
	previewQueries := getPreviewQueries(config.Crawler.PreviewQueries)
	heuristics := getAboutHeuristics(config.Data.Heuristics)

	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("Request URL:", r.Request.URL, "failed with response:", r, "\nError:", err)
	})

	// on every a element which has an href attribute, call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {

		if e.Response.StatusCode >= 400 || e.Response.StatusCode <= 100 {
			return
		}

		link := getLink(e.Attr("href"))
		if findSuffix(SUFFIXES, link) {
			return
		}

		link = e.Request.AbsoluteURL(link)
		u, err := url.Parse(link)
		if err != nil {
			return
		}

		outgoingDomain := u.Hostname()
		currentDomain := e.Request.URL.Hostname()

		// log which site links to what
		if !util.Contains(boringWords, link) && !util.Contains(boringDomains, link) {
			if !find(domains, outgoingDomain) {
				fmt.Println("non-webring-link", link, e.Request.URL, linkDepths[e.Request.URL.String()])
				// solidarity! someone in the webring linked to someone else in it
			} else if outgoingDomain != currentDomain && outgoingDomain != initialDomain && currentDomain != initialDomain {
				fmt.Println("webring-link", link, e.Request.URL, linkDepths[e.Request.URL.String()])
			}
		}

		// rule-based crawling
		var pathsite string
		for _, s := range pathsites {
			if strings.Contains(s, outgoingDomain) {
				pathsite = s
				break
			}
		}
		// the visited site was a so called »pathsite», a site with restrictions on which pages can be crawled (most often due to
		// existing on a shared domain)
		if pathsite != "" {
			// make sure we're only crawling descendents of the original path
			if strings.HasPrefix(link, pathsite) {
				q.AddURL(link)
			}
		} else {
			// visits links from AllowedDomains
			q.AddURL(link)
		}
	})

	handleIndexing(c, previewQueries, heuristics, linkDepths)

	// start scraping
	q.Run(c)
}
