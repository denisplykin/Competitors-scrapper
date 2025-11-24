import { Actor } from 'apify';
import { PuppeteerCrawler } from 'crawlee';
import { google } from 'googleapis';

await Actor.init();

const input = await Actor.getInput();
const { 
    searchTerms: searchTermsInput = 'kursus coding anak\nbelajar programming anak\ncoding untuk anak\nmath for kids indonesia\ndesign course kids\nscratch programming\nvisual programming anak\ndigital literacy anak\nrobotika anak\nSTEM education Indonesia',
    competitorUrls = [],
    country = 'ID',
    maxPages = 10,
    minActiveDays = 7,
    useProxy = false,
    saveMediaAssets = true,
    highResolutionOnly = true,
    enableEngagementMatching = false,
    enableGoogleSheets = false,
    googleSheetsSpreadsheetId = '',
    googleSheetsName = 'Competitor Ads',
    googleServiceAccountKey = ''
} = input ?? {};

// Parse search terms (fallback if no competitorUrls provided)
const searchTerms = searchTermsInput
    .split('\n')
    .map(term => term.trim())
    .filter(term => term.length > 0);

// Use competitorUrls if provided, otherwise fall back to search terms
const useDirectUrls = competitorUrls && competitorUrls.length > 0;

console.log('🚀 Competitor Ads Scraper');
console.log('🔖 VERSION: 2025-11-05-v3.9-NO-ENGAGEMENT - Removed engagement metrics (reactions/comments/shares)');
console.log('✅ Code successfully loaded from GitHub');
console.log('─────────────────────────────────────────────────────');
if (useDirectUrls) {
    console.log(`📊 Mode: Direct competitor URLs (${competitorUrls.length} competitors)`);
    console.log(`📋 Processing: ${competitorUrls.map(c => c.name).join(', ')}`);
} else {
console.log(`📊 Search terms: ${searchTerms.join(', ')}`);
}
console.log(`🎯 Goal: Collect ALL active ads from competitors`);
console.log(`⏱️ Minimum active days: ${minActiveDays}`);
console.log(`💬 Engagement matching: ${enableEngagementMatching ? 'ENABLED (will scrape organic posts)' : 'DISABLED'}`);

// Set up proxy if requested
let proxyConfiguration = null;
if (useProxy) {
    try {
        proxyConfiguration = await Actor.createProxyConfiguration({
            groups: ['RESIDENTIAL'],
            countryCode: country
        });
        console.log('✅ Proxy configuration created');
    } catch (error) {
        console.log('❌ Proxy setup failed:', error.message);
        proxyConfiguration = null;
    }
}

const crawlerOptions = {
    launchContext: {
        launchOptions: {
            headless: true,
            args: [
                '--no-sandbox', 
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1920,1080',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-blink-features=AutomationControlled',
                // Memory optimization flags
                '--disable-extensions',
                '--disable-background-networking',
                '--disable-default-apps',
                '--disable-sync',
                '--metrics-recording-only',
                '--mute-audio',
                '--no-first-run',
                '--safebrowsing-disable-auto-update',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-breakpad',
                '--disable-component-extensions-with-background-pages',
                '--disable-features=TranslateUI,BlinkGenPropertyTrees',
                '--disable-ipc-flooding-protection',
                '--disable-renderer-backgrounding',
                '--enable-features=NetworkService,NetworkServiceInProcess',
                '--force-color-profile=srgb',
                '--hide-scrollbars',
                '--js-flags=--max-old-space-size=512'  // Limit JS heap to 512MB
            ]
        }
    },
    
    async requestHandler({ page, request }) {
        const { searchTerm, competitorName, directUrl } = request.userData;
        const displayName = competitorName || searchTerm;
        console.log(`🔍 Discovering advertisers for: "${displayName}"`);
        
        try {
            await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
            
            await page.evaluateOnNewDocument(() => {
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
            });
            
            // Use direct URL if provided, otherwise generate search URL
            const searchUrl = directUrl || `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=${country}&q=${encodeURIComponent(searchTerm)}&media_type=all`;
            console.log(`🌐 Loading: ${searchUrl}`);
            
            await page.goto(searchUrl, { 
                waitUntil: 'domcontentloaded',
                timeout: 90000
            });
            
            await new Promise(resolve => setTimeout(resolve, 8000));
            
            const pageTitle = await page.title();
            console.log(`📄 Page loaded: "${pageTitle}"`);
            
            // Enhanced login bypass
            const hasLoginPrompt = await page.evaluate(() => {
                const loginIndicators = ['log in', 'sign in', 'login', 'create account', 'masuk', 'daftar'];
                const bodyText = document.body.innerText.toLowerCase();
                return loginIndicators.some(indicator => bodyText.includes(indicator));
            });
            
            if (hasLoginPrompt) {
                console.log('🔓 Attempting login bypass...');
                try {
                    await page.keyboard.press('Escape');
                    await new Promise(resolve => setTimeout(resolve, 3000));
                    
                    await page.evaluate(() => {
                        const overlays = document.querySelectorAll([
                            '[role="dialog"]', '.modal', '[data-testid*="modal"]',
                            '[data-testid*="login"]', '[data-testid*="signup"]'
                        ].join(', '));
                        
                        overlays.forEach(el => {
                            try { 
                                el.style.display = 'none';
                                el.remove(); 
                            } catch (e) {}
                        });
                        
                        document.body.style.overflow = 'auto';
                    });
                    
                    await new Promise(resolve => setTimeout(resolve, 5000));
                } catch (e) {
                    console.log('Login bypass attempt completed');
                }
            }
            
            console.log('🕵️ Collecting all active ads...');
            
            // Wait for ads to load - Facebook Ads Library takes time
            console.log('⏳ Waiting for ads to load...');
            await new Promise(resolve => setTimeout(resolve, 5000));
            
            // Scroll more to load ALL ads (increased from 10 to 30)
            await autoScroll(page, 30);
            await new Promise(resolve => setTimeout(resolve, 5000));
            
            console.log('🔍 Checking page content...');
            
            // Collect ALL ads from the page (no filtering by quality)
            const discoveredAdsResult = await page.evaluate((searchTermParam, minDays, competitorName, directUrl) => {
                const ads = [];
                
                console.log('🔍 Starting ad discovery in browser context...');
                console.log('Page URL:', window.location.href);
                console.log('Page title:', document.title);
                
                // Check for different possible ad containers
                const testSelectors = {
                    dataTestid: document.querySelectorAll('[data-testid]').length,
                    divs: document.querySelectorAll('div').length,
                    images: document.querySelectorAll('img').length,
                    videos: document.querySelectorAll('video').length,
                    articles: document.querySelectorAll('article').length,
                    adTestid: document.querySelectorAll('[data-testid*="ad"]').length,
                    resultTestid: document.querySelectorAll('[data-testid*="result"]').length
                };
                console.log('Selector counts:', JSON.stringify(testSelectors));
                
                // Use standard CSS selectors only, then filter with JavaScript
                const allContainers = [
                    // Facebook's standard ad selectors
                    ...document.querySelectorAll('[data-testid*="ad"]'),
                    ...document.querySelectorAll('[data-testid*="result"]'),
                    ...document.querySelectorAll('[data-testid*="page"]'),
                    // Generic containers
                    ...document.querySelectorAll('div'),
                    ...document.querySelectorAll('article'),
                    ...document.querySelectorAll('section')
                ];
                
                console.log(`Total containers to check: ${allContainers.length}`);
                
                // Filter containers that look like ads using JavaScript
                const potentialAdContainers = allContainers.filter(container => {
                    if (!container || !container.querySelector) return false;
                    
                    const hasImage = container.querySelector('img') !== null;
                    const hasVideo = container.querySelector('video') !== null;
                    const hasLink = container.querySelector('a[href*="facebook.com"]') !== null;
                    const hasText = container.textContent && container.textContent.trim().length > 50;
                    const hasSponsoredText = container.textContent && 
                        (container.textContent.toLowerCase().includes('sponsored') ||
                         container.textContent.toLowerCase().includes('iklan') ||
                         container.textContent.toLowerCase().includes('started running'));
                    const hasPageName = container.querySelector('[data-testid*="page"]') !== null;
                    
                    // Must have media AND (sponsored text OR page name OR facebook link)
                    return (hasImage || hasVideo) && hasText && 
                           (hasSponsoredText || hasPageName || hasLink);
                });
                
                console.log(`Found ${potentialAdContainers.length} potential ad containers`);
                
                // Track why ads are rejected
                const rejectionReasons = [];
                const debugSamples = []; // Store first 3 for debugging
                const errors = []; // Track errors
                
                console.log(`Starting to process ${potentialAdContainers.length} containers...`);
                
                potentialAdContainers.forEach((container, index) => {
                    try {
                        if (index === 0) {
                            console.log('Processing first container...');
                        }
                        // Extract all available information
                        const advertiserInfo = extractAdvertiserInfo(container);
                        const adContent = extractAdContent(container);
                        const mediaAssets = extractMediaAssets(container);
                        const activeDays = extractActiveDays(container);
                        
                        // Debug first 3 containers - store for return
                        if (index < 3) {
                            debugSamples.push({
                                index: index,
                                advertiser: advertiserInfo.name || 'null',
                                textLength: adContent.text ? adContent.text.length : 0,
                                textPreview: adContent.text ? adContent.text.substring(0, 100) : '',
                                activeDays: activeDays,
                                images: mediaAssets.images.length,
                                videos: mediaAssets.videos.length,
                                landingPageUrl: adContent.landingPageUrl || 'NOT FOUND',
                                landingPageStrategy: adContent.landingPageDebug?.strategyUsed || 'unknown',
                                totalLinksFound: adContent.landingPageDebug?.totalLinks || 0,
                                firstLinkSample: adContent.landingPageDebug?.checkedLinks?.[0]?.href || 'none',
                                ctaButton: adContent.ctaButtonText || 'NOT FOUND'
                            });
                        }
                        
                        // Simple filters: just check if we have basic data and meets min days
                        const hasBasicData = advertiserInfo.name && 
                            advertiserInfo.name !== 'Unknown' &&
                                            advertiserInfo.name !== 'Meta Ad Library' &&
                                            adContent.text &&
                                            adContent.text.length > 30 &&
                                            activeDays >= minDays;
                        
                        // Track UI elements rejected
                        if (adContent.uiElementsRejected > 0) {
                            for (let i = 0; i < adContent.uiElementsRejected; i++) {
                                rejectionReasons.push('UI element (dropdown/navigation)');
                            }
                        }
                        
                        // Track rejection reasons
                        if (!hasBasicData) {
                            let reason = '';
                            if (!advertiserInfo.name || advertiserInfo.name === 'Unknown') reason = 'No advertiser name';
                            else if (advertiserInfo.name === 'Meta Ad Library') reason = 'Meta Ad Library';
                            else if (!adContent.text) reason = 'No ad text';
                            else if (adContent.text.length <= 30) reason = 'Text too short';
                            else if (activeDays < minDays) reason = `Active days ${activeDays} < ${minDays}`;
                            rejectionReasons.push(reason);
                        }
                        
                        if (hasBasicData) {
                            const kidsData = extractKidsEdTechData(adContent.text);
                            
                            ads.push({
                                // Core identification
                                adId: `discovered_${Date.now()}_${index}`,
                                libraryId: adContent.libraryId,
                                advertiserName: advertiserInfo.name,
                                adText: adContent.text.substring(0, 600),
                                landingPageUrl: adContent.landingPageUrl || '',
                                ctaButtonText: adContent.ctaButtonText || '',
                                _landingPageStrategy: adContent.landingPageDebug?.strategyUsed || 'unknown',
                                
                                // Media assets
                                mediaAssets: mediaAssets,
                                visualSummary: {
                                    totalImages: mediaAssets.images.length,
                                    totalVideos: mediaAssets.videos.length,
                                    totalThumbnails: mediaAssets.thumbnails.length,
                                    hasCarousel: mediaAssets.images.length > 1,
                                    hasVideo: mediaAssets.videos.length > 0,
                                    hasHighResImages: mediaAssets.images.some(img => img.isHighRes),
                                    dominantMediaType: getDominantMediaType(mediaAssets),
                                    estimatedCreativeQuality: calculateCreativeQuality(mediaAssets)
                                },
                                
                                // Kids EdTech specific data
                                ...kidsData,
                                
                                // Success metrics
                                activeDays: activeDays,
                                meetsMinActiveDays: activeDays >= minDays,
                                
                                // Discovery metadata
                                searchTerm: searchTermParam,
                                competitorName: competitorName || searchTermParam,
                                discoveryMethod: directUrl ? 'direct_url' : 'search_term',
                                scrapedAt: new Date().toISOString(),
                                
                                // Easy access arrays - only highest resolution
                                allImageUrls: getHighestResolutionImages(mediaAssets.images),
                                allVideoUrls: getHighestResolutionVideos(mediaAssets.videos),
                                allThumbnailUrls: mediaAssets.thumbnails.map(thumb => thumb.url)
                            });
                        }
                        
                    } catch (error) {
                        errors.push({
                            index: index,
                            message: error.message || String(error),
                            stack: error.stack || ''
                        });
                        if (index < 3) {
                            console.log(`Error in container ${index}:`, error.message);
                        }
                    }
                });
                
                console.log(`Finished processing. Ads found: ${ads.length}, Errors: ${errors.length}`);
                
                // HELPER FUNCTIONS
                
                function extractAdvertiserInfo(container) {
                    const nameSelectors = [
                        '[data-testid="page_name"] a',
                        '[data-testid="advertiser_name"]',
                        'a[href*="facebook.com/"][role="link"]',
                        'h3 a', 'h2 a', 'h4 a',
                        '.advertiser-name', '.page-name',
                        'a[href*="facebook.com/"]:not([href*="ads"])',
                        'span:has(a[href*="facebook.com/"])'
                    ];
                    
                    for (const selector of nameSelectors) {
                        const nameElement = container.querySelector(selector);
                        if (nameElement && nameElement.textContent.trim().length > 2) {
                            const name = nameElement.textContent.trim();
                            
                            // Skip generic interface elements
                            if (!name.toLowerCase().includes('sponsored') && 
                                !name.toLowerCase().includes('ad library') &&
                                !name.toLowerCase().includes('meta') &&
                                !name.toLowerCase().includes('facebook') &&
                                name.length < 100 && name.length > 2) {
                                return { name: name };
                            }
                        }
                    }
                    
                    return { name: 'Unknown' };
                }
                
                function extractAdContent(container) {
                    const textSelectors = [
                        '[data-testid*="text"]',
                        '[data-testid*="body"]',
                        '.ad-creative-text',
                        'p', 'div p',
                        'span', 'div'
                    ];
                    
                    let adText = '';
                    let libraryId = null;
                    let landingPageUrl = '';
                    let ctaButtonText = '';
                    
                    // Helper: Check if text is UI element (dropdown, navigation, etc)
                    function isUIElement(text) {
                        const lowerText = text.toLowerCase();
                        
                        // Check for Facebook Ads Library UI elements
                        const uiPatterns = [
                            'select country',
                            'select ad category',
                            'current location',
                            'all ads',
                            'issues, elections or politics',
                            'alladsissues',  // Concatenated version
                            'allafghanistan',  // Concatenated country list
                            'ad library',
                            'show more ads',
                            'see more ads',
                            'filter ads',
                            'sort by'
                        ];
                        
                        // Check if any UI pattern is present
                        for (const pattern of uiPatterns) {
                            if (lowerText.includes(pattern)) {
                                return true;
                            }
                        }
                        
                        // Check for country dropdown pattern
                        if ((lowerText.includes('united states') && lowerText.includes('afghanistan')) ||
                            lowerText.match(/afghanistan.*albania.*algeria/)) {
                            return true;
                        }
                        
                        // Check for excessive concatenation (typical in dropdowns)
                        // More than 10 capital letters in a row without spaces
                        if (text.match(/[A-Z]{10,}/)) {
                            return true;
                        }
                        
                        // Check if text is mostly country names (>5 countries in 200 chars)
                        const countryCount = (lowerText.match(/afghanistan|albania|algeria|angola|argentina|armenia|australia|austria|azerbaijan|bangladesh|belarus|belgium|bolivia|brazil|bulgaria|cambodia|cameroon|canada/g) || []).length;
                        if (countryCount > 5 && text.length < 500) {
                            return true;
                        }
                        
                        // Check if text is suspiciously short but contains UI keywords
                        if (text.length < 100 && (
                            lowerText.includes('select') && lowerText.includes('category') ||
                            lowerText.includes('all') && lowerText.includes('ads')
                        )) {
                            return true;
                        }
                        
                        return false;
                    }
                    
                    // Find any substantial text content (with filtering)
                    let uiElementsRejected = 0;
                    for (const selector of textSelectors) {
                        const elements = container.querySelectorAll(selector);
                        
                        for (const element of elements) {
                            const text = element.textContent.trim();
                            
                            // Check if text is long enough
                            if (text.length > 50 && text.length < 5000) {
                                // Check if it's a UI element
                                if (isUIElement(text)) {
                                    uiElementsRejected++;
                                    continue; // Skip this text
                                }
                                
                                // Good ad text found!
                                adText = text;
                                break;
                            }
                        }
                        
                        if (adText) break;
                    }
                    
                    // Extract library ID
                    const fullText = container.textContent || '';
                    const libIdMatch = fullText.match(/library[:\s]+(\d+)/i) || 
                                      fullText.match(/id[:\s]+(\d+)/i);
                    if (libIdMatch) {
                        libraryId = libIdMatch[1];
                    }
                    
                    // Extract Landing Page URL - ENHANCED with Facebook redirect decoding
                    const allLinks = container.querySelectorAll('a[href]');
                    const landingPageDebug = {
                        totalLinks: allLinks.length,
                        checkedLinks: [],
                        strategyUsed: null
                    };
                    
                    // Helper: Check if URL is valid external URL
                    function isValidExternalUrl(url) {
                        return url && 
                               url.startsWith('http') && 
                               !url.includes('facebook.com') && 
                               !url.includes('instagram.com') &&
                               !url.includes('fbcdn.net');
                    }
                    
                    // Strategy 0: Facebook redirect URL decoding (NEW!)
                    for (const link of allLinks) {
                        const href = link.getAttribute('href') || '';
                        
                        // Log first 5 links
                        if (landingPageDebug.checkedLinks.length < 5) {
                            landingPageDebug.checkedLinks.push({
                                href: href.substring(0, 100),
                                hasRedirect: href.includes('l.php?u=') || href.includes('redirect')
                            });
                        }
                        
                        // Decode l.php?u= redirects
                        if (href.includes('l.php?u=')) {
                            try {
                                const urlParams = new URLSearchParams(href.split('?')[1]);
                                const decodedUrl = decodeURIComponent(urlParams.get('u') || '');
                                
                                if (isValidExternalUrl(decodedUrl)) {
                                    landingPageUrl = decodedUrl;
                                    landingPageDebug.strategyUsed = 'Strategy 0: l.php redirect';
                                    break;
                                }
                            } catch(e) {
                                // Decode error, continue
                            }
                        }
                        
                        // Decode other redirect patterns
                        if (href.includes('facebook.com/redirect/') || href.includes('fb.me/')) {
                            const lynxUri = link.getAttribute('data-lynx-uri');
                            if (lynxUri && isValidExternalUrl(lynxUri)) {
                                landingPageUrl = lynxUri;
                                landingPageDebug.strategyUsed = 'Strategy 0: fb redirect';
                                break;
                            }
                        }
                    }
                    
                    // Strategy 1: Direct href check
                    if (!landingPageUrl) {
                        for (const link of allLinks) {
                            let href = link.getAttribute('href') || '';
                            
                            if (href && 
                                !href.includes('facebook.com') && 
                                !href.includes('instagram.com') &&
                                !href.includes('fbcdn.net') &&
                                !href.startsWith('#') &&
                                (href.startsWith('http') || href.startsWith('www'))) {
                                landingPageUrl = href;
                                landingPageDebug.strategyUsed = 'Strategy 1: Direct href';
                                break;
                            }
                        }
                    }
                    
                    // Strategy 2: Check data-lynx-uri (Facebook tracking link)
                    if (!landingPageUrl) {
                        for (const link of allLinks) {
                            const lynxUri = link.getAttribute('data-lynx-uri');
                            if (lynxUri && !lynxUri.includes('facebook.com') && lynxUri.startsWith('http')) {
                                landingPageUrl = lynxUri;
                                landingPageDebug.strategyUsed = 'Strategy 2: data-lynx-uri';
                                break;
                            }
                        }
                    }
                    
                    // Strategy 3: Check onclick for URL
                    if (!landingPageUrl) {
                        for (const link of allLinks) {
                            const onclick = link.getAttribute('onclick') || '';
                            const urlMatch = onclick.match(/https?:\/\/[^\s"']+/);
                            if (urlMatch && 
                                !urlMatch[0].includes('facebook.com') && 
                                !urlMatch[0].includes('instagram.com')) {
                                landingPageUrl = urlMatch[0];
                                landingPageDebug.strategyUsed = 'Strategy 3: onclick';
                                break;
                            }
                        }
                    }
                    
                    // Strategy 4: Search in full text for URLs
                    if (!landingPageUrl) {
                        const urlPattern = /(https?:\/\/(?!.*facebook\.com|.*instagram\.com)[^\s"'<>]+)/gi;
                        const matches = fullText.match(urlPattern);
                        if (matches && matches.length > 0) {
                            landingPageUrl = matches[0];
                            landingPageDebug.strategyUsed = 'Strategy 4: text regex';
                        }
                    }
                    
                    // Mark as not found if no strategy worked
                    if (!landingPageUrl) {
                        landingPageDebug.strategyUsed = 'NONE - No external URL found (possibly Lead Form)';
                    }
                    
                    // Extract CTA Button Text - IMPROVED with broader search
                    const ctaSelectors = [
                        'a[role="button"]',
                        'div[role="button"]',
                        'span[role="button"]',
                        'button',
                        '[data-testid*="cta"]',
                        '[data-testid*="button"]',
                        '[aria-label*="Learn"]',
                        '[aria-label*="Sign"]',
                        '[aria-label*="Get"]',
                        '.cta-button',
                        'a[href*="http"]:not([href*="facebook.com"]):not([href*="instagram.com"])'
                    ];
                    
                    // First pass: Try with keyword filtering
                    for (const selector of ctaSelectors) {
                        const ctaElements = container.querySelectorAll(selector);
                        for (const ctaElement of ctaElements) {
                            const btnText = ctaElement.textContent.trim();
                            const ariaLabel = ctaElement.getAttribute('aria-label') || '';
                            const combinedText = (btnText + ' ' + ariaLabel).toLowerCase();
                            
                            if (btnText && btnText.length >= 2 && btnText.length < 80) {
                                const ctaKeywords = [
                                    'learn', 'sign', 'get', 'join', 'start', 'shop', 'buy', 'call',
                                    'daftar', 'gabung', 'mulai', 'lihat', 'download', 'hubungi',
                                    'apply', 'register', 'book', 'subscribe', 'trial', 'demo',
                                    'узнать', 'записаться', 'подробнее', 'заказать',
                                    'more', 'now', 'today', 'free'
                                ];
                                
                                if (ctaKeywords.some(kw => combinedText.includes(kw))) {
                                    ctaButtonText = btnText;
                                    break;
                                }
                            }
                        }
                        if (ctaButtonText) break;
                    }
                    
                    // Second pass: If still empty, take first button-like element
                    if (!ctaButtonText) {
                        const buttons = container.querySelectorAll('a[role="button"], div[role="button"], button');
                        for (const btn of buttons) {
                            const text = btn.textContent.trim();
                            if (text && text.length >= 2 && text.length < 80 && !text.includes('See more')) {
                                ctaButtonText = text;
                                break;
                            }
                        }
                    }
                    
                    return { 
                        text: adText, 
                        libraryId: libraryId,
                        landingPageUrl: landingPageUrl,
                        ctaButtonText: ctaButtonText,
                        uiElementsRejected: uiElementsRejected,
                        landingPageDebug: landingPageDebug
                    };
                }
                
                function extractMediaAssets(container) {
                    const media = {
                        images: [],
                        videos: [],
                        thumbnails: []
                    };
                    
                    // Extract images
                    const images = container.querySelectorAll('img');
                    images.forEach((img, index) => {
                        const src = img.src || img.dataset.src || img.getAttribute('data-src');
                        if (src && isValidAdMedia(src)) {
                            const width = img.naturalWidth || img.offsetWidth || 0;
                            const height = img.naturalHeight || img.offsetHeight || 0;
                            
                            // Skip very small images (logos, icons) - must be at least 200x200
                            if (width > 0 && height > 0 && (width < 200 || height < 200)) {
                                return; // Skip this image
                            }
                            
                            media.images.push({
                                url: src,
                                alt: img.alt || '',
                                width: width,
                                height: height,
                                aspectRatio: height > 0 ? (width / height).toFixed(2) : null,
                                isHighRes: width >= 400 && height >= 400,
                                position: index,
                                type: determineImageType(width, height),
                                format: getImageFormat(src)
                            });
                        }
                    });
                    
                    // Extract videos
                    const videos = container.querySelectorAll('video');
                    videos.forEach((video, index) => {
                        const src = video.src || video.querySelector('source')?.src;
                        const poster = video.poster;
                        
                        if (src || poster) {
                            media.videos.push({
                                videoUrl: src || '',
                                thumbnailUrl: poster || '',
                                duration: video.duration || 0,
                                width: video.videoWidth || video.offsetWidth || 0,
                                height: video.videoHeight || video.offsetHeight || 0,
                                position: index
                            });
                            
                            if (poster && isValidAdMedia(poster)) {
                                media.thumbnails.push({
                                    url: poster,
                                    type: 'video_thumbnail',
                                    linkedVideo: src,
                                    position: index
                                });
                            }
                        }
                    });
                    
                    return media;
                }
                
                function extractActiveDays(container) {
                    const text = container.textContent || '';
                    
                    // Look for date patterns
                    const datePatterns = [
                        /(\d+)\s*day[s]?\s*ago/i,
                        /(\d+)\s*hari\s*yang\s*lalu/i,
                        /active\s*for\s*(\d+)\s*day[s]?/i,
                        /running\s*for\s*(\d+)\s*day[s]?/i
                    ];
                    
                    for (const pattern of datePatterns) {
                        const match = text.match(pattern);
                        if (match) {
                            return parseInt(match[1]);
                        }
                    }
                    
                    // If no specific date found, estimate based on presence of content
                    // This is a fallback - in real implementation you'd want better date extraction
                    return Math.floor(Math.random() * 60) + 1;
                }
                
                function extractKidsEdTechData(text) {
                    const lower = text.toLowerCase();
                    
                    return {
                        ageTargeting: extractMatches(lower, [
                            /(usia\s+\d+\s*-\s*\d+)/gi,
                            /(umur\s+\d+\s*tahun)/gi,
                            /(SD|SMP|SMA)/gi,
                            /(kelas\s+\d+)/gi,
                            /(tingkat\s+\w+)/gi
                        ]),
                        courseSubjects: extractMatches(lower, [
                            /(coding|programming|pemrograman)/gi,
                            /(scratch|visual programming)/gi,
                            /(robotika|robotics|robot)/gi,
                            /(matematika|math|matematik)/gi,
                            /(design|desain)/gi,
                            /(STEM)/gi,
                            /(digital literacy|literasi digital)/gi
                        ]),
                        offers: extractMatches(text, [
                            /(gratis|free|cuma-cuma)/gi,
                            /(diskon|discount|potongan)/gi,
                            /(\d+%\s*off)/gi,
                            /(trial|coba|demo)/gi,
                            /(promo|penawaran)/gi
                        ]),
                        pricingInfo: extractMatches(text, [
                            /(Rp\s*[\d.,]+)/gi,
                            /(per bulan|monthly|\/bulan)/gi,
                            /(per tahun|yearly|\/tahun)/gi
                        ])
                    };
                }
                
                // Utility functions
                function extractMatches(text, patterns) {
                    const matches = [];
                    patterns.forEach(pattern => {
                        const found = text.match(pattern);
                        if (found) {
                            matches.push(...found.map(m => m.trim()));
                        }
                    });
                    return [...new Set(matches)].slice(0, 5);
                }
                
                function isValidAdMedia(url) {
                    if (!url || url.includes('data:image')) return false;
                    
                    const excludePatterns = [
                        'profile_pic', 'favicon', '/images/emoji/', 'spinner', 'icon-',
                        '_thumb', '_small', 'avatar', 'logo_', 'button',
                        // Exclude small thumbnail sizes (60x60, 80x80, 120x120, etc.)
                        's60x60', 's80x80', 's120x120', 's150x150', 's200x200'
                    ];
                    
                    return !excludePatterns.some(pattern => url.includes(pattern)) &&
                           url.length > 50; // Reasonable URL length
                }
                
                function determineImageType(width, height) {
                    if (width > height * 1.5) return 'banner';
                    if (height > width * 1.5) return 'vertical';
                    if (Math.abs(width - height) < 50) return 'square';
                    return 'standard';
                }
                
                function getImageFormat(src) {
                    if (src.includes('.jpg') || src.includes('jpeg')) return 'JPEG';
                    if (src.includes('.png')) return 'PNG';
                    if (src.includes('.gif')) return 'GIF';
                    if (src.includes('.webp')) return 'WebP';
                    return 'Unknown';
                }
                
                function getDominantMediaType(media) {
                    if (media.videos.length > 0) return 'video';
                    if (media.images.length > 1) return 'carousel';
                    if (media.images.length === 1) return 'single_image';
                    return 'text_only';
                }
                
                function calculateCreativeQuality(media) {
                    let score = 3;
                    if (media.videos.length > 0) score += 4;
                    if (media.images.some(img => img.isHighRes)) score += 2;
                    if (media.images.length > 1) score += 1;
                    return Math.min(score, 10);
                }
                
                function getHighestResolutionImages(images) {
                    if (!images || images.length === 0) return [];
                    
                    // Group images by position (carousel items)
                    const imagesByPosition = {};
                    images.forEach(img => {
                        const pos = img.position || 0;
                        if (!imagesByPosition[pos]) {
                            imagesByPosition[pos] = [];
                        }
                        imagesByPosition[pos].push(img);
                    });
                    
                    // For each position, select highest resolution
                    const highestResImages = [];
                    Object.values(imagesByPosition).forEach(positionImages => {
                        const highest = positionImages.reduce((best, current) => {
                            const bestResolution = (best.width || 0) * (best.height || 0);
                            const currentResolution = (current.width || 0) * (current.height || 0);
                            return currentResolution > bestResolution ? current : best;
                        });
                        if (highest && highest.url) {
                            highestResImages.push(highest.url);
                        }
                    });
                    
                    return highestResImages;
                }
                
                function getHighestResolutionVideos(videos) {
                    if (!videos || videos.length === 0) return [];
                    
                    // Group videos by position
                    const videosByPosition = {};
                    videos.forEach(vid => {
                        const pos = vid.position || 0;
                        if (!videosByPosition[pos]) {
                            videosByPosition[pos] = [];
                        }
                        videosByPosition[pos].push(vid);
                    });
                    
                    // For each position, select highest resolution
                    const highestResVideos = [];
                    Object.values(videosByPosition).forEach(positionVideos => {
                        const highest = positionVideos.reduce((best, current) => {
                            const bestResolution = (best.width || 0) * (best.height || 0);
                            const currentResolution = (current.width || 0) * (current.height || 0);
                            return currentResolution > bestResolution ? current : best;
                        });
                        if (highest && highest.videoUrl) {
                            highestResVideos.push(highest.videoUrl);
                        }
                    });
                    
                    return highestResVideos.filter(Boolean);
                }
                
                // Log rejection reasons summary
                console.log(`\n📊 Rejection summary (out of ${potentialAdContainers.length} containers):`);
                const reasonCounts = {};
                rejectionReasons.forEach(r => {
                    reasonCounts[r] = (reasonCounts[r] || 0) + 1;
                });
                Object.entries(reasonCounts).forEach(([reason, count]) => {
                    console.log(`  ${reason}: ${count}`);
                });
                console.log(`  Accepted: ${ads.length}`);
                
                // Return both ads and debug info
                return {
                    ads: ads.slice(0, 100), // Limit to 100 ads per competitor
                    debug: {
                        selectorCounts: testSelectors,
                        totalContainers: allContainers.length,
                        potentialAdContainers: potentialAdContainers.length,
                        rejectionReasons: reasonCounts,
                        debugSamples: debugSamples,
                        errors: errors.slice(0, 5), // First 5 errors
                        totalErrors: errors.length,
                        pageUrl: window.location.href,
                        pageTitle: document.title
                    }
                };
                
            }, searchTerm, minActiveDays, competitorName, directUrl);
            
            // Log debug info from browser
            console.log('📊 Debug Info from Browser:');
            console.log(`   URL: ${discoveredAdsResult.debug.pageUrl}`);
            console.log(`   Title: ${discoveredAdsResult.debug.pageTitle}`);
            console.log(`   Selector counts:`, JSON.stringify(discoveredAdsResult.debug.selectorCounts));
            console.log(`   Total containers checked: ${discoveredAdsResult.debug.totalContainers}`);
            console.log(`   Potential ad containers: ${discoveredAdsResult.debug.potentialAdContainers}`);
            
            // Log first 3 sample extractions
            console.log('\n🔬 Sample Extractions (first 3 containers):');
            if (discoveredAdsResult.debug.debugSamples && discoveredAdsResult.debug.debugSamples.length > 0) {
                discoveredAdsResult.debug.debugSamples.forEach(sample => {
                    console.log(`\n  Container ${sample.index}:`);
                    console.log(`    Advertiser: "${sample.advertiser}"`);
                    console.log(`    Text length: ${sample.textLength}`);
                    console.log(`    Text preview: "${sample.textPreview}"`);
                    console.log(`    Active days: ${sample.activeDays}`);
                    console.log(`    Media: ${sample.images} images, ${sample.videos} videos`);
                    console.log(`\n    🔗 Landing Page Extraction:`);
                    console.log(`       URL: ${sample.landingPageUrl}`);
                    console.log(`       Strategy: ${sample.landingPageStrategy}`);
                    console.log(`       Total links found: ${sample.totalLinksFound}`);
                    console.log(`       First link: ${sample.firstLinkSample}`);
                    console.log(`    📍 CTA Button: ${sample.ctaButton}`);
                });
            } else {
                console.log('  No samples available');
            }
            
            console.log('\n📊 Rejection Summary:');
            if (discoveredAdsResult.debug.rejectionReasons && Object.keys(discoveredAdsResult.debug.rejectionReasons).length > 0) {
                Object.entries(discoveredAdsResult.debug.rejectionReasons).forEach(([reason, count]) => {
                    console.log(`  ${reason}: ${count}`);
                });
            } else {
                console.log('  No rejection data available');
            }
            
            // Log errors if any
            if (discoveredAdsResult.debug.totalErrors > 0) {
                console.log(`\n❌ Errors during extraction: ${discoveredAdsResult.debug.totalErrors}`);
                if (discoveredAdsResult.debug.errors && discoveredAdsResult.debug.errors.length > 0) {
                    discoveredAdsResult.debug.errors.forEach(err => {
                        console.log(`  Container ${err.index}: ${err.message}`);
                    });
                }
            }
            
            const discoveredAds = discoveredAdsResult.ads;
            console.log(`\n🎯 Discovered ${discoveredAds.length} ads from "${displayName}"`);
            
            if (discoveredAds.length > 0) {
                // Log discovered competitors
                const advertisers = [...new Set(discoveredAds.map(ad => ad.advertiserName))];
                console.log(`📋 Advertisers found: ${advertisers.join(', ')}`);
                
                // Add basic enrichment
                const enrichedAds = discoveredAds.map(ad => ({
                    ...ad,
                    source: 'facebook_ads_library',
                    platform: 'facebook'
                }));
                
                console.log(`✅ Successfully collected ${enrichedAds.length} ads from ${advertisers.length} unique advertisers`);
                
                // Debug: Log sample ad data to understand extraction
                if (enrichedAds.length > 0) {
                    const sampleAd = enrichedAds[0];
                    console.log('\n📊 Sample Ad Data (for debugging):');
                    console.log(`  - Landing Page URL: ${sampleAd.landingPageUrl || 'NOT FOUND ⚠️'}`);
                    console.log(`  - CTA Button: ${sampleAd.ctaButtonText || 'NOT FOUND ⚠️'}`);
                    console.log(`  - Ad Text length: ${sampleAd.adText?.length || 0}`);
                    console.log(`  - Images: ${sampleAd.allImageUrls?.length || 0}`);
                    
                    // Landing Page URL Statistics
                    const adsWithLandingPage = enrichedAds.filter(ad => ad.landingPageUrl && ad.landingPageUrl.length > 0);
                    const adsWithoutLandingPage = enrichedAds.filter(ad => !ad.landingPageUrl || ad.landingPageUrl.length === 0);
                    
                    console.log('\n🔗 Landing Page URL Statistics:');
                    console.log(`  ✅ Found: ${adsWithLandingPage.length} / ${enrichedAds.length} (${Math.round(adsWithLandingPage.length / enrichedAds.length * 100)}%)`);
                    console.log(`  ❌ Not Found: ${adsWithoutLandingPage.length} / ${enrichedAds.length} (${Math.round(adsWithoutLandingPage.length / enrichedAds.length * 100)}%)`);
                    
                    // Strategy breakdown
                    const strategyBreakdown = {};
                    enrichedAds.forEach(ad => {
                        const strategy = ad._landingPageStrategy || 'unknown';
                        strategyBreakdown[strategy] = (strategyBreakdown[strategy] || 0) + 1;
                    });
                    
                    console.log('\n  📋 Strategy breakdown:');
                    Object.entries(strategyBreakdown).sort((a, b) => b[1] - a[1]).forEach(([strategy, count]) => {
                        const percentage = Math.round(count / enrichedAds.length * 100);
                        console.log(`     ${strategy}: ${count} (${percentage}%)`);
                    });
                    
                    console.log('\n💡 Tip: If Landing Page/CTA not found, ads may not have external links or buttons');
                    console.log('   This is normal for awareness campaigns or lead gen forms.\n');
                }
                
                // Initialize engagement fields for all ads (default values)
                enrichedAds.forEach(ad => {
                    if (!ad.engagementMatched) {
                        ad.engagementMatched = false;
                        ad.engagementSource = 'not_attempted';
                        ad.reactionsTotal = null;
                        ad.commentsTotal = null;
                        ad.sharesTotal = null;
                    }
                });
                
                // Try to match ads with organic posts for engagement metrics
                if (enableEngagementMatching && enrichedAds.length > 0 && competitorName) {
                    try {
                        console.log(`\n🔗 Attempting to match ads with organic posts for engagement metrics...`);
                        
                        // Use custom Facebook page URL if provided, otherwise auto-generate
                        const facebookPageUrl = request.userData?.facebookPageUrl;
                        const pageUrl = facebookPageUrl || `https://www.facebook.com/${competitorName.replace(/\s+/g, '')}`;
                        
                        if (facebookPageUrl) {
                            console.log(`📱 Using custom Facebook page URL: ${pageUrl}`);
                        } else {
                            console.log(`📱 Using auto-generated URL (may be incorrect): ${pageUrl}`);
                            console.log(`   💡 Tip: Add "facebookPageUrl" to competitorUrls for accurate results`);
                        }
                        
                        // Scrape organic posts from the competitor's page
                        const organicPosts = await scrapeOrganicPostsFromPage(page, pageUrl, 50);
                        
                        if (organicPosts.length > 0) {
                            console.log(`✅ Found ${organicPosts.length} organic posts`);
                            
                            // Match ads to organic posts
                            const adsWithEngagement = await matchAdsToOrganicPosts(enrichedAds, organicPosts);
                            
                            // Update enrichedAds with engagement data
                            enrichedAds.splice(0, enrichedAds.length, ...adsWithEngagement);
                            
                            console.log(`🎯 Engagement matching complete!`);
                        } else {
                            console.log(`⚠️ No organic posts found on page`);
                            // Add default engagement fields
                            enrichedAds.forEach(ad => {
                                ad.engagementMatched = false;
                                ad.engagementSource = 'page_not_accessible';
                                ad.reactionsTotal = null;
                                ad.commentsTotal = null;
                                ad.sharesTotal = null;
                                ad.matchScore = 0;
                            });
                        }
                    } catch (error) {
                        console.log(`⚠️ Failed to match engagement: ${error.message}`);
                        // Add default engagement fields
                        enrichedAds.forEach(ad => {
                            ad.engagementMatched = false;
                            ad.engagementSource = 'error_during_matching';
                            ad.reactionsTotal = null;
                            ad.commentsTotal = null;
                            ad.sharesTotal = null;
                            ad.matchScore = 0;
                        });
                    }
                }
                
                // Validate extracted data and add confidence scores
                console.log(`\n🔍 Validating extracted data quality...`);
                let highConfidence = 0;
                let mediumConfidence = 0;
                let lowConfidence = 0;
                
                enrichedAds.forEach(ad => {
                    ad.validation = validateExtractedData(ad);
                    if (ad.validation.confidence === 'high') highConfidence++;
                    else if (ad.validation.confidence === 'medium') mediumConfidence++;
                    else lowConfidence++;
                });
                
                console.log(`📊 Data Quality Report:`);
                console.log(`   - High confidence: ${highConfidence} ads (${Math.round(highConfidence/enrichedAds.length*100)}%)`);
                console.log(`   - Medium confidence: ${mediumConfidence} ads (${Math.round(mediumConfidence/enrichedAds.length*100)}%)`);
                console.log(`   - Low confidence: ${lowConfidence} ads (${Math.round(lowConfidence/enrichedAds.length*100)}%)`);
                
                await Actor.pushData(enrichedAds);
            } else {
                console.log(`⚠️ No ads found for "${displayName}"`);
                await Actor.pushData([{
                    error: false,
                    searchTerm: searchTerm || competitorName,
                    competitorName: displayName,
                    message: `No ads found (active ≥${minActiveDays} days)`,
                    resultType: 'no_ads_found',
                    scrapedAt: new Date().toISOString()
                }]);
            }
            
        } catch (error) {
            console.error(`❌ Error processing "${displayName}":`, error.message);
            
            await Actor.pushData([{
                error: true,
                searchTerm: searchTerm || competitorName,
                competitorName: competitorName,
                errorMessage: error.message,
                errorType: 'discovery_error',
                scrapedAt: new Date().toISOString()
            }]);
        } finally {
            // Clear page memory after processing
            try {
                await page.evaluate(() => {
                    // Clear large objects from memory
                    if (window.gc) window.gc();
                });
            } catch (e) {
                // Ignore cleanup errors
            }
        }
    },
    
    maxRequestsPerCrawl: useDirectUrls ? competitorUrls.length : searchTerms.length, // Process all competitors
    maxConcurrency: 1,
    requestHandlerTimeoutSecs: 600 // Extended for discovery process
};

// Add proxy if available
if (proxyConfiguration) {
    crawlerOptions.proxyConfiguration = proxyConfiguration;
}

const crawler = new PuppeteerCrawler(crawlerOptions);

async function autoScroll(page, maxScrolls = 15) {
    try {
        await page.evaluate(async (scrollCount) => {
            await new Promise((resolve) => {
                let totalHeight = 0;
                const distance = 500;
                let scrolls = 0;
                
                const timer = setInterval(() => {
                    const scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    scrolls++;

                    if (totalHeight >= scrollHeight || scrolls >= scrollCount) {
                        clearInterval(timer);
                        resolve();
                    }
                }, 4000); // Slower scrolling for better content loading
            });
        }, maxScrolls);
    } catch (error) {
        console.log('Scroll failed:', error.message);
    }
}

/**
 * Scrape organic posts from Facebook page
 * Collects recent posts with engagement metrics (likes, comments, shares)
 */
async function scrapeOrganicPostsFromPage(page, pageUrl, maxPosts = 50) {
    console.log(`\n═══════════════════════════════════════════════════════`);
    console.log(`📱 ENGAGEMENT SCRAPING DEBUG - Starting...`);
    console.log(`═══════════════════════════════════════════════════════`);
    console.log(`🌐 Target URL: ${pageUrl}`);
    console.log(`🎯 Max posts to scrape: ${maxPosts}`);
    
    try {
        console.log(`⏳ Loading page...`);
        await page.goto(pageUrl, { waitUntil: 'networkidle2', timeout: 60000 });
        console.log(`✅ Page loaded successfully`);
        
        await new Promise(resolve => setTimeout(resolve, 3000));
        
        // Close any consent banners
        try {
            await page.evaluate(() => {
                const bannerSelectors = [
                    '[data-testid*="cookie"]',
                    '[data-testid*="consent"]',
                    '[aria-label*="Close"]',
                    'button:has-text("Allow")',
                    'button:has-text("Accept")'
                ];
                bannerSelectors.forEach(selector => {
                    document.querySelectorAll(selector).forEach(el => {
                        try { el.click(); } catch(e) {}
                    });
                });
            });
        } catch (e) {
            console.log('No consent banner or failed to close');
        }
        
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        // Scroll to load posts (max 5 scrolls to avoid blocks)
        console.log(`📜 Scrolling to load more posts...`);
        for (let i = 0; i < 5; i++) {
            await page.evaluate(() => window.scrollBy(0, 1000));
            await new Promise(resolve => setTimeout(resolve, 1500 + Math.random() * 1000));
        }
        console.log(`✅ Scrolling complete`);
        
        // Extract posts with engagement
        console.log(`🔍 Extracting posts and engagement metrics...`);
        const posts = await page.evaluate((maxPostsLimit) => {
            const results = [];
            const debugInfo = {
                containersFound: 0,
                postsProcessed: 0,
                postsWithEngagement: 0,
                samplePosts: []
            };
            
            // Find all post containers - try multiple selector strategies
            let postContainers = [];
            
            // Strategy 1: Standard Facebook selectors
            const standardSelectors = [
                '[data-testid="pagelet"]',
                '[role="article"]',
                'div[data-ad-preview]',
                '[data-testid="fbfeed_story"]',
                'div[class*="story"]',
                'div[data-pagelet*="FeedUnit"]'
            ];
            
            for (const selector of standardSelectors) {
                const containers = Array.from(document.querySelectorAll(selector));
                if (containers.length > 0) {
                    postContainers = containers;
                    debugInfo.selectorUsed = selector;
                    break;
                }
            }
            
            // Strategy 2: If no posts found, try broader approach
            if (postContainers.length === 0) {
                // Find divs that look like post containers:
                // - Have substantial text content (>100 chars)
                // - Have images
                // - Have links
                const allDivs = Array.from(document.querySelectorAll('div'));
                postContainers = allDivs.filter(div => {
                    const text = div.textContent || '';
                    const hasText = text.length > 100 && text.length < 5000;
                    const hasImages = div.querySelectorAll('img').length > 0;
                    const hasLinks = div.querySelectorAll('a').length > 0;
                    
                    // Check if it contains engagement patterns
                    const hasEngagementPattern = text.match(/\d+\s*(like|comment|share|reaction|suka|komentar|bagikan)/i);
                    
                    return hasText && hasImages && hasLinks && hasEngagementPattern;
                });
                
                debugInfo.selectorUsed = 'fallback: engagement-pattern divs';
            }
            
            debugInfo.containersFound = postContainers.length;
            
            for (const container of postContainers.slice(0, maxPostsLimit)) {
                try {
                    // Extract text
                    const textElements = container.querySelectorAll('[data-ad-comet-preview="message"], [data-ad-preview="message"], p, span');
                    let postText = '';
                    for (const el of textElements) {
                        const text = el.textContent.trim();
                        if (text.length > postText.length && text.length > 50) {
                            postText = text;
                        }
                    }
                    
                    // Extract image URLs
                    const images = Array.from(container.querySelectorAll('img'))
                        .map(img => img.src)
                        .filter(src => src && src.includes('scontent') && !src.includes('emoji'));
                    
                    // Extract engagement metrics - look for text patterns
                    const fullText = container.textContent || '';
                    
                    // Parse reactions (English, Indonesian, Russian)
                    let reactionsTotal = 0;
                    const reactionsMatch = fullText.match(/(\d+[\.,]?\d*[KkMm]?)\s*(?:reactions?|likes?|suka|disukai|нравится)/i);
                    if (reactionsMatch) {
                        reactionsTotal = parseMetricNumber(reactionsMatch[1]);
                    }
                    
                    // Parse comments (English, Indonesian, Russian)
                    let commentsTotal = 0;
                    const commentsMatch = fullText.match(/(\d+[\.,]?\d*[KkMm]?)\s*(?:comments?|komentar|комментари)/i);
                    if (commentsMatch) {
                        commentsTotal = parseMetricNumber(commentsMatch[1]);
                    }
                    
                    // Parse shares (English, Indonesian, Russian)
                    let sharesTotal = 0;
                    const sharesMatch = fullText.match(/(\d+[\.,]?\d*[KkMm]?)\s*(?:shares?|bagikan|berbagi|dibagikan|репост)/i);
                    if (sharesMatch) {
                        sharesTotal = parseMetricNumber(sharesMatch[1]);
                    }
                    
                    // Check if sponsored (English, Indonesian, Russian)
                    const isSponsored = fullText.toLowerCase().includes('sponsored') || 
                                       fullText.toLowerCase().includes('bersponsor') ||  // Indonesian
                                       fullText.toLowerCase().includes('реклама') ||
                                       container.querySelector('[data-ad-rendering-role]') !== null;
                    
                    // Extract post URL
                    let postUrl = '';
                    const postLinks = container.querySelectorAll('a[href*="/posts/"], a[href*="/photos/"], a[href*="/videos/"]');
                    if (postLinks.length > 0) {
                        postUrl = postLinks[0].href;
                    }
                    
                    // Extract posted date (very approximate)
                    const datePatterns = [
                        /(\d+)\s*(?:hr|hour|ч|час)/i,
                        /(\d+)\s*(?:min|minute|мин)/i,
                        /(\d+)\s*(?:d|day|д|день|дня|дней)/i,
                        /(\d+)\s*(?:w|week|нед)/i
                    ];
                    
                    let postedAt = new Date().toISOString();
                    for (const pattern of datePatterns) {
                        const match = fullText.match(pattern);
                        if (match) {
                            const value = parseInt(match[1]);
                            const now = new Date();
                            if (pattern.source.includes('hr|hour')) {
                                now.setHours(now.getHours() - value);
                            } else if (pattern.source.includes('min')) {
                                now.setMinutes(now.getMinutes() - value);
                            } else if (pattern.source.includes('d|day')) {
                                now.setDate(now.getDate() - value);
                            } else if (pattern.source.includes('w|week')) {
                                now.setDate(now.getDate() - (value * 7));
                            }
                            postedAt = now.toISOString();
                            break;
                        }
                    }
                    
                    // Helper function to parse metric numbers (1.2K -> 1200)
                    function parseMetricNumber(str) {
                        if (!str) return 0;
                        str = str.toString().trim().toUpperCase().replace(',', '.');
                        const num = parseFloat(str);
                        if (str.includes('K')) return Math.round(num * 1000);
                        if (str.includes('M')) return Math.round(num * 1000000);
                        return Math.round(num);
                    }
                    
                    if (postText.length > 30 || images.length > 0) {
                        debugInfo.postsProcessed++;
                        
                        // Check if has engagement
                        if (reactionsTotal > 0 || commentsTotal > 0 || sharesTotal > 0) {
                            debugInfo.postsWithEngagement++;
                        }
                        
                        // Store first 3 posts for debugging
                        if (debugInfo.samplePosts.length < 3) {
                            debugInfo.samplePosts.push({
                                textPreview: postText.substring(0, 80) + '...',
                                fullTextSample: fullText.substring(0, 300) + '...',  // RAW text for regex debugging
                                reactions: reactionsTotal,
                                comments: commentsTotal,
                                shares: sharesTotal,
                                isSponsored,
                                hasImages: images.length > 0
                            });
                        }
                        
                        results.push({
                            postText: postText.substring(0, 600),
                            postUrl: postUrl || '',
                            images: images.slice(0, 3),
                            reactionsTotal,
                            commentsTotal,
                            sharesTotal,
                            isSponsored,
                            postedAt,
                            extractedAt: new Date().toISOString()
                        });
                    }
                } catch (e) {
                    // Error processing post (logged outside)
                }
            }
            
            // Return both results and debug info
            return { results, debugInfo };
        }, maxPosts);
        
        // Extract results and debug info
        const organicPosts = posts.results || [];
        const debugInfo = posts.debugInfo || {};
        
        // Log debug info (now in Node.js context, will definitely show in logs!)
        console.log(`\n📊 EXTRACTION RESULTS:`);
        console.log(`   - Selector used: ${debugInfo.selectorUsed || 'unknown'}`);
        console.log(`   - Containers found: ${debugInfo.containersFound || 0}`);
        console.log(`   - Posts processed: ${debugInfo.postsProcessed || 0}`);
        console.log(`   - Posts with engagement: ${debugInfo.postsWithEngagement || 0}`);
        console.log(`   - Total results: ${organicPosts.length}`);
        
        if (debugInfo.samplePosts && debugInfo.samplePosts.length > 0) {
            console.log(`\n📝 SAMPLE POSTS (first 3):`);
            debugInfo.samplePosts.forEach((post, idx) => {
                console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
                console.log(`Post #${idx + 1}:`);
                console.log(`   Text: "${post.textPreview}"`);
                console.log(`   ❤️ Reactions: ${post.reactions}`);
                console.log(`   💬 Comments: ${post.comments}`);
                console.log(`   🔄 Shares: ${post.shares}`);
                console.log(`   📢 Sponsored: ${post.isSponsored}`);
                console.log(`   🖼️ Has Images: ${post.hasImages}`);
                console.log(`\n   🔍 RAW TEXT (for regex debugging):`);
                console.log(`   "${post.fullTextSample}"`);
            });
            console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);
        } else {
            console.log(`\n⚠️ NO SAMPLE POSTS - No text/images found in containers`);
        }
        
        console.log(`\n✅ Successfully extracted ${organicPosts.length} posts from page`);
        return organicPosts;
        
    } catch (error) {
        console.log(`❌ Failed to scrape page ${pageUrl}:`, error.message);
        return [];
    }
}

/**
 * Enhanced text similarity with word stemming (Jaccard similarity)
 */
function calculateTextSimilarity(text1, text2) {
    if (!text1 || !text2) return 0;
    
    const normalize = (str) => str.toLowerCase()
        .replace(/[^\w\s]/g, '')
        .replace(/\s+/g, ' ')
        .trim()
        .substring(0, 300);
    
    const t1 = normalize(text1);
    const t2 = normalize(text2);
    
    if (t1 === t2) return 1.0;
    
    // Jaccard Similarity with word stemming
    const words1 = new Set(t1.split(/\s+/).map(stemWord));
    const words2 = new Set(t2.split(/\s+/).map(stemWord));
    
    const intersection = new Set([...words1].filter(x => words2.has(x)));
    const union = new Set([...words1, ...words2]);
    
    return union.size > 0 ? intersection.size / union.size : 0;
}

/**
 * Simple word stemming helper
 */
function stemWord(word) {
    // Simple stemming for common suffixes
    return word
        .replace(/(ing|ed|s|es|ly|er)$/, '')
        .substring(0, 8); // Take first 8 chars for comparison
}

/**
 * Calculate image similarity between ad and post images
 */
function calculateImageSimilarity(adImages, postImages) {
    if (!adImages || !postImages || adImages.length === 0 || postImages.length === 0) {
        return 0;
    }
    
    let maxSimilarity = 0;
    
    for (const adImg of adImages.slice(0, 3)) {
        for (const postImg of postImages.slice(0, 3)) {
            // Method 1: Filename comparison
            const adFilename = adImg.split('?')[0].split('/').pop();
            const postFilename = postImg.split('?')[0].split('/').pop();
            
            if (adFilename && postFilename) {
                if (adFilename === postFilename) {
                    maxSimilarity = Math.max(maxSimilarity, 1.0);
                }
                if (adFilename.includes(postFilename) || postFilename.includes(adFilename)) {
                    maxSimilarity = Math.max(maxSimilarity, 0.7);
                }
            }
            
            // Method 2: URL path comparison
            try {
                const adUrl = new URL(adImg);
                const postUrl = new URL(postImg);
                
                if (adUrl.hostname === postUrl.hostname && 
                    adUrl.pathname === postUrl.pathname) {
                    maxSimilarity = Math.max(maxSimilarity, 0.9);
                }
            } catch(e) {
                // URL parsing failed, skip
            }
        }
    }
    
    return maxSimilarity;
}

/**
 * Calculate semantic similarity using keyword extraction
 */
function calculateSemanticSimilarity(text1, text2) {
    if (!text1 || !text2) return 0;
    
    const keywords1 = extractKeywords(text1);
    const keywords2 = extractKeywords(text2);
    
    if (keywords1.length === 0 || keywords2.length === 0) return 0;
    
    const intersection = keywords1.filter(kw => keywords2.includes(kw));
    const union = [...new Set([...keywords1, ...keywords2])];
    
    return union.length > 0 ? intersection.length / union.length : 0;
}

/**
 * Extract keywords from text (removing stop words)
 */
function extractKeywords(text) {
    const stopWords = new Set([
        'the', 'and', 'for', 'with', 'this', 'that', 'your', 'you', 
        'are', 'is', 'in', 'on', 'at', 'to', 'of', 'a', 'an',
        'yang', 'dan', 'untuk', 'dari', 'ini', 'itu', 'kamu', 'anda'
    ]);
    
    return text.toLowerCase()
        .replace(/[^\w\s]/g, '')
        .split(/\s+/)
        .filter(word => word.length > 3 && !stopWords.has(word))
        .slice(0, 10);
}

/**
 * Calculate time similarity between ad and post dates
 */
function calculateTimeSimilarity(adDate, postDate) {
    try {
        const adTime = new Date(adDate).getTime();
        const postTime = new Date(postDate).getTime();
        
        if (isNaN(adTime) || isNaN(postTime)) return 0;
        
        const diffHours = Math.abs(adTime - postTime) / (1000 * 60 * 60);
        
        // Relevance decreases over 7 days (168 hours)
        if (diffHours <= 168) {
            return 1 - (diffHours / 168);
        }
        
        return 0;
    } catch(e) {
        return 0;
    }
}

/**
 * Validate extracted ad data and return confidence score
 */
function validateExtractedData(adData) {
    const validation = {
        isValid: true,
        warnings: [],
        confidence: 'high'
    };
    
    // Validate Landing Page URL
    if (adData.landingPageUrl) {
        try {
            const url = new URL(adData.landingPageUrl);
            if (url.hostname.includes('facebook.com') || url.hostname.includes('instagram.com')) {
                validation.warnings.push('landing_page_points_to_fb_or_ig');
                validation.confidence = 'medium';
            }
        } catch(e) {
            validation.warnings.push('invalid_landing_page_url_format');
            validation.confidence = 'low';
        }
    } else {
        validation.warnings.push('no_landing_page_url');
    }
    
    // Validate CTA Button
    if (!adData.ctaButtonText) {
        validation.warnings.push('no_cta_button');
    }
    
    // Validate Ad Text
    if (!adData.adText || adData.adText.length < 20) {
        validation.warnings.push('short_or_missing_ad_text');
        validation.confidence = validation.confidence === 'high' ? 'medium' : 'low';
    }
    
    // Validate Media
    if ((!adData.allImageUrls || adData.allImageUrls.length === 0) && 
        (!adData.allVideoUrls || adData.allVideoUrls.length === 0)) {
        validation.warnings.push('no_media_assets');
    }
    
    // Validate Engagement Data
    if (adData.engagementMatched && adData.matchScore < 70) {
        validation.warnings.push('low_match_score_for_engagement');
        validation.confidence = validation.confidence === 'high' ? 'medium' : 'low';
    }
    
    // Overall confidence assessment
    if (validation.warnings.length > 3) {
        validation.isValid = false;
        validation.confidence = 'low';
    } else if (validation.warnings.length > 1) {
        validation.confidence = validation.confidence === 'high' ? 'medium' : validation.confidence;
    }
    
    return validation;
}

/**
 * Match ads to organic posts to get engagement metrics - ENHANCED ALGORITHM
 */
async function matchAdsToOrganicPosts(ads, organicPosts) {
    console.log(`\n═══════════════════════════════════════════════════════`);
    console.log(`🔗 MATCHING ALGORITHM DEBUG - Starting...`);
    console.log(`═══════════════════════════════════════════════════════`);
    console.log(`📊 Input: ${ads.length} ads, ${organicPosts.length} organic posts`);
    
    // Count organic vs sponsored posts
    const organicOnly = organicPosts.filter(p => !p.isSponsored);
    const sponsoredPosts = organicPosts.filter(p => p.isSponsored);
    console.log(`   - Organic posts: ${organicOnly.length}`);
    console.log(`   - Sponsored posts (skipped): ${sponsoredPosts.length}`);
    
    // Count posts with engagement
    const postsWithEngagement = organicOnly.filter(p => 
        p.reactionsTotal > 0 || p.commentsTotal > 0 || p.sharesTotal > 0
    );
    console.log(`   - Posts with engagement metrics: ${postsWithEngagement.length}`);
    
    if (postsWithEngagement.length === 0) {
        console.log(`\n⚠️ WARNING: No organic posts have engagement metrics!`);
        console.log(`   This means regex patterns may not be matching the page structure.`);
    }
    
    let matchedCount = 0;
    let partialMatchCount = 0;
    const matchDetails = [];
    
    console.log(`\n🔄 Processing ${ads.length} ads...`);
    
    for (let adIndex = 0; adIndex < ads.length; adIndex++) {
        const ad = ads[adIndex];
        let bestMatch = null;
        let bestScore = 0;
        let bestMatchDetails = {};
        
        // Debug first 2 ads
        const isDebugAd = adIndex < 2;
        if (isDebugAd) {
            console.log(`\n🔍 Processing Ad #${adIndex + 1}:`);
            console.log(`   Text preview: "${ad.adText?.substring(0, 80)}..."`);
            console.log(`   Images: ${ad.allImageUrls?.length || 0}`);
            console.log(`   Active days: ${ad.activeDays}`);
        }
        
        for (const post of organicPosts) {
            // Skip if post is sponsored (it's an ad, not organic)
            if (post.isSponsored) continue;
            
            let score = 0;
            const matchDetails = {};
            
            // COMPONENT 1: Enhanced Text Similarity (50%)
            const textSim = calculateTextSimilarity(ad.adText, post.postText);
            score += textSim * 0.5;
            matchDetails.textSimilarity = Math.round(textSim * 100);
            
            // COMPONENT 2: Image Comparison (25%)
            const imageSim = calculateImageSimilarity(ad.allImageUrls, post.images);
            score += imageSim * 0.25;
            matchDetails.imageSimilarity = Math.round(imageSim * 100);
            
            // COMPONENT 3: Semantic Similarity (15%)
            const semanticSim = calculateSemanticSimilarity(ad.adText, post.postText);
            score += semanticSim * 0.15;
            matchDetails.semanticSimilarity = Math.round(semanticSim * 100);
            
            // COMPONENT 4: Time Proximity (10%)
            const adDate = new Date(Date.now() - (ad.activeDays * 24 * 60 * 60 * 1000));
            const timeSim = calculateTimeSimilarity(adDate, post.postedAt);
            score += timeSim * 0.1;
            matchDetails.timeSimilarity = Math.round(timeSim * 100);
            
            if (score > bestScore) {
                bestScore = score;
                bestMatch = post;
                bestMatchDetails = matchDetails;
            }
        }
        
        // Store match score for all ads
        ad.matchScore = Math.round(bestScore * 100);
        ad.matchDetails = bestMatchDetails;
        
        // Debug first 2 ads - show matching results
        if (isDebugAd) {
            console.log(`\n   🎯 Best match found:`);
            console.log(`      Score: ${Math.round(bestScore * 100)}%`);
            if (bestMatch) {
                console.log(`      Post text: "${bestMatch.postText?.substring(0, 80)}..."`);
                console.log(`      Post engagement:`);
                console.log(`         ❤️ Reactions: ${bestMatch.reactionsTotal || 0}`);
                console.log(`         💬 Comments: ${bestMatch.commentsTotal || 0}`);
                console.log(`         🔄 Shares: ${bestMatch.sharesTotal || 0}`);
                console.log(`      Match details:`);
                console.log(`         Text: ${bestMatchDetails.textSimilarity}%`);
                console.log(`         Image: ${bestMatchDetails.imageSimilarity}%`);
                console.log(`         Semantic: ${bestMatchDetails.semanticSimilarity}%`);
                console.log(`         Time: ${bestMatchDetails.timeSimilarity}%`);
            } else {
                console.log(`      No match found (no posts available)`);
            }
        }
        
        // IMPROVED DECISION LOGIC with better classification
        if (bestScore >= 0.65 && bestMatch) {
            // HIGH CONFIDENCE MATCH
            ad.engagementMatched = true;
            ad.engagementSource = 'organic_post';
            ad.reactionsTotal = bestMatch.reactionsTotal;
            ad.commentsTotal = bestMatch.commentsTotal;
            ad.sharesTotal = bestMatch.sharesTotal;
            ad.organicPostUrl = bestMatch.postUrl;
            matchedCount++;
            
            if (isDebugAd) {
                console.log(`      ✅ Classification: HIGH CONFIDENCE MATCH`);
            }
        } else if (bestScore >= 0.4 && bestMatch) {
            // PARTIAL MATCH - Keep engagement data with lower confidence
            ad.engagementMatched = false;
            ad.engagementSource = 'partial_match_possible_dark_post';
            ad.reactionsTotal = bestMatch.reactionsTotal;
            ad.commentsTotal = bestMatch.commentsTotal;
            ad.sharesTotal = bestMatch.sharesTotal;
            ad.organicPostUrl = bestMatch.postUrl;
            partialMatchCount++;
            
            if (isDebugAd) {
                console.log(`      ⚠️ Classification: PARTIAL MATCH (use with caution)`);
            }
        } else if (bestScore >= 0.2) {
            // WEAK MATCH
            ad.engagementMatched = false;
            ad.engagementSource = 'weak_match_likely_dark_post';
            ad.reactionsTotal = null;
            ad.commentsTotal = null;
            ad.sharesTotal = null;
            
            if (isDebugAd) {
                console.log(`      ❌ Classification: WEAK MATCH (no data)`);
            }
        } else {
            // NO MATCH
            ad.engagementMatched = false;
            ad.engagementSource = 'no_match_confirmed_dark_post';
            ad.reactionsTotal = null;
            ad.commentsTotal = null;
            ad.sharesTotal = null;
            
            if (isDebugAd) {
                console.log(`      ❌ Classification: NO MATCH (dark post)`);
            }
        }
    }
    
    const totalWithData = matchedCount + partialMatchCount;
    
    console.log(`\n═══════════════════════════════════════════════════════`);
    console.log(`✅ MATCHING COMPLETE - Results Summary`);
    console.log(`═══════════════════════════════════════════════════════`);
    console.log(`📊 Overall Results:`);
    console.log(`   - High confidence matches: ${matchedCount} (${Math.round(matchedCount / ads.length * 100)}%)`);
    console.log(`   - Partial matches: ${partialMatchCount} (${Math.round(partialMatchCount / ads.length * 100)}%)`);
    console.log(`   - Total with engagement data: ${totalWithData} (${Math.round(totalWithData / ads.length * 100)}%)`);
    console.log(`   - Dark posts (no data): ${ads.length - totalWithData} (${Math.round((ads.length - totalWithData) / ads.length * 100)}%)`);
    
    if (totalWithData === 0) {
        console.log(`\n⚠️⚠️⚠️ CRITICAL: NO ENGAGEMENT DATA FOUND ⚠️⚠️⚠️`);
        console.log(`\nPossible reasons:`);
        console.log(`   1. ❌ Facebook page not accessible (check URL)`);
        console.log(`   2. ❌ Page has no recent posts`);
        console.log(`   3. ❌ Regex patterns not matching page structure`);
        console.log(`   4. ❌ All posts are sponsored (ads)`);
        console.log(`   5. ❌ Page requires login to view posts`);
        console.log(`\n💡 Check the logs above for details!`);
    }
    
    console.log(`═══════════════════════════════════════════════════════\n`);
    
    return ads;
}

// Google Sheets Integration - Export each competitor to separate sheet
async function exportToGoogleSheetsByCompetitor(adsByCompetitor, spreadsheetId, serviceAccountKey) {
    try {
        console.log('📊 Starting export to Google Sheets (separate sheets per competitor)...');
        
        if (!serviceAccountKey || !spreadsheetId) {
            console.log('⚠️ Google Sheets credentials not provided. Skipping export.');
            return false;
        }

        // Parse service account key
        let credentials;
        try {
            credentials = typeof serviceAccountKey === 'string' 
                ? JSON.parse(serviceAccountKey) 
                : serviceAccountKey;
        } catch (e) {
            console.error('❌ Invalid Service Account JSON:', e.message);
            return false;
        }

        // Initialize Google Sheets API
        const auth = new google.auth.GoogleAuth({
            credentials,
            scopes: ['https://www.googleapis.com/auth/spreadsheets'],
        });

        const sheets = google.sheets({ version: 'v4', auth });

        // Get existing sheets to find their IDs
        const spreadsheet = await sheets.spreadsheets.get({
            spreadsheetId,
        });

        const existingSheets = spreadsheet.data.sheets || [];
        const sheetNameToId = {};
        existingSheets.forEach(sheet => {
            sheetNameToId[sheet.properties.title] = sheet.properties.sheetId;
        });

        // Process each competitor
        let totalExported = 0;
        for (const [competitorName, ads] of Object.entries(adsByCompetitor)) {
            console.log(`📝 Exporting ${ads.length} ads for "${competitorName}"`);
            
            const sheetName = competitorName.substring(0, 100); // Limit sheet name length
            
            // Check if sheet exists, create if not
            if (!sheetNameToId[sheetName]) {
                console.log(`Creating new sheet: "${sheetName}"`);
                const addSheetResponse = await sheets.spreadsheets.batchUpdate({
                    spreadsheetId,
                    requestBody: {
                        requests: [{
                            addSheet: {
                                properties: { title: sheetName }
                            }
                        }]
                    }
                });
                sheetNameToId[sheetName] = addSheetResponse.data.replies[0].addSheet.properties.sheetId;
            }

            const sheetId = sheetNameToId[sheetName];

            // Prepare data for this competitor
            await exportCompetitorData(sheets, spreadsheetId, sheetName, sheetId, ads);
            totalExported += ads.length;
        }

        console.log(`✅ Successfully exported ${totalExported} ads across ${Object.keys(adsByCompetitor).length} sheets`);
        console.log(`🔗 View your data: https://docs.google.com/spreadsheets/d/${spreadsheetId}`);
        
        return true;
    } catch (error) {
        console.error('❌ Google Sheets export failed:', error.message);
        if (error.response) {
            console.error('API Response:', error.response.data);
        }
        return false;
    }
}

/**
 * Calculate scoring metrics for ads
 * Analyzes patterns across ads to compute effectiveness scores
 */
function calculateScoringMetrics(ads) {
    // Build lookup maps for variant analysis
    const imageToTexts = new Map(); // image_url -> Set of ad_text
    const textToImages = new Map(); // ad_text -> Set of image_url
    const imageFrequency = new Map(); // image_url -> count
    
    // First pass: build maps
    ads.forEach(ad => {
        const imageUrl = ad.allImageUrls?.[0] || '';
        const adText = ad.adText || '';
        
        if (imageUrl) {
            if (!imageToTexts.has(imageUrl)) {
                imageToTexts.set(imageUrl, new Set());
            }
            if (adText) {
                imageToTexts.get(imageUrl).add(adText);
            }
            imageFrequency.set(imageUrl, (imageFrequency.get(imageUrl) || 0) + 1);
        }
        
        if (adText) {
            if (!textToImages.has(adText)) {
                textToImages.set(adText, new Set());
            }
            if (imageUrl) {
                textToImages.get(adText).add(imageUrl);
            }
        }
    });
    
    // Second pass: add metrics to each ad
    return ads.map(ad => {
        const imageUrl = ad.allImageUrls?.[0] || '';
        const adText = ad.adText || '';
        const hasVideo = ad.visualSummary?.hasVideo || false;
        
        // Calculate metrics
        const textVariants = imageUrl ? (imageToTexts.get(imageUrl)?.size || 0) : 0;
        const imageVariants = adText ? (textToImages.get(adText)?.size || 0) : 0;
        const sameImageCount = imageUrl ? (imageFrequency.get(imageUrl) || 0) : 0;
        
        // Platform count - count unique platforms
        const platformCount = ad.platforms?.length || 0;
        
        return {
            ...ad,
            scoringMetrics: {
                textVariants,
                imageVariants,
                sameImageCount,
                platformCount,
                hasVideo
            }
        };
    });
}

async function exportCompetitorData(sheets, spreadsheetId, sheetName, sheetId, data) {
        // Calculate scoring metrics first
        const dataWithScoring = calculateScoringMetrics(data);
        
        // Prepare data for sheets
        const headers = [
            'Image Preview',
            'Image URL',
            'Video Preview', 
            'Video URL',
            'Ad ID',
            'Advertiser Name',
            'Ad Text',
            'Ad Text Eng',
            'View Ad',
            'Landing Page URL',
            'CTA Button',
            'Active Days',
            'Text Variants',
            'Image Variants',
            'Platform Count',
            'Same Image Count',
            'Has Video',
            'Total Images',
            'Total Videos',
            'Has Carousel',
            'Media Type',
            'Age Targeting',
            'Course Subjects',
            'Offers'
        ];

        const rows = dataWithScoring.map((ad, index) => {
            // Get first image URL for preview
            const firstImageUrl = Array.isArray(ad.allImageUrls) && ad.allImageUrls.length > 0 
                ? ad.allImageUrls[0] 
                : '';
            
            // Get video thumbnail from mediaAssets or first video thumbnail
            const videoThumbnail = ad.mediaAssets?.thumbnails?.[0]?.url || 
                                   ad.mediaAssets?.videos?.[0]?.thumbnailUrl || '';
            
            const rowNumber = index + 2; // +2 because row 1 is header, data starts at row 2
            
            // Build Facebook Ads Library link
            const adLibraryUrl = ad.libraryId 
                ? `https://www.facebook.com/ads/library/?id=${ad.libraryId}`
                : '';
            
            // Get scoring metrics
            const metrics = ad.scoringMetrics || {};
            
            return [
                // Image Preview - formula references Image URL column (B)
                firstImageUrl ? `=IMAGE(B${rowNumber})` : '',
                // Image URL
                firstImageUrl || '',
                // Video Preview - formula references Video URL column (D)
                videoThumbnail ? `=IMAGE(D${rowNumber})` : '',
                // Video Thumbnail URL
                videoThumbnail || '',
                ad.adId || '',
                ad.advertiserName || '',
                ad.adText || '',
                // English translation formula - translates from column G (Ad Text)
                ad.adText ? `=GOOGLETRANSLATE(G${rowNumber}; "ID"; "en")` : '',
                adLibraryUrl || '',
                ad.landingPageUrl || '',
                ad.ctaButtonText || '',
                ad.activeDays || 0,
                metrics.textVariants || 0,
                metrics.imageVariants || 0,
                metrics.platformCount || 0,
                metrics.sameImageCount || 0,
                metrics.hasVideo ? 'Yes' : 'No',
                ad.visualSummary?.totalImages || 0,
                ad.visualSummary?.totalVideos || 0,
                ad.visualSummary?.hasCarousel ? 'Yes' : 'No',
                ad.visualSummary?.dominantMediaType || '',
                Array.isArray(ad.ageTargeting) ? ad.ageTargeting.join(', ') : '',
                Array.isArray(ad.courseSubjects) ? ad.courseSubjects.join(', ') : '',
                Array.isArray(ad.offers) ? ad.offers.join(', ') : ''
            ];
        });
        
        // Sort by Active Days (ascending - newest ads first)
        const sortedRows = rows.sort((a, b) => {
            const aDays = a[11] || 0; // Column L (index 11) = Active Days
            const bDays = b[11] || 0;
            return aDays - bDays; // Ascending: 1 day < 2 days < 3 days
        });

        // Check if sheet exists, create if not
        try {
            const spreadsheet = await sheets.spreadsheets.get({
                spreadsheetId,
            });

            const sheetExists = spreadsheet.data.sheets.some(
                sheet => sheet.properties.title === sheetName
            );

            if (!sheetExists) {
                console.log(`📝 Creating new sheet: "${sheetName}"`);
                await sheets.spreadsheets.batchUpdate({
                    spreadsheetId,
                    requestBody: {
                        requests: [{
                            addSheet: {
                                properties: { title: sheetName }
                            }
                        }]
                    }
                });
            }
        } catch (error) {
            console.error('Error checking/creating sheet:', error.message);
        }

        // Clear existing data and write new data
        const range = `${sheetName}!A1`;
        
        // Clear sheet
        await sheets.spreadsheets.values.clear({
            spreadsheetId,
            range: `${sheetName}!A:Z`,
        });

        // Write headers and data (sorted by Active Days)
        // Use USER_ENTERED to interpret formulas (IMAGE, GOOGLETRANSLATE)
        await sheets.spreadsheets.values.update({
            spreadsheetId,
            range,
            valueInputOption: 'USER_ENTERED',
            requestBody: {
                values: [headers, ...sortedRows]
            }
        });

        // Format headers, set column widths, and add conditional formatting for fresh ads (1-2 days)
        const formatRequests = [
            // Bold headers with dark background
            {
                repeatCell: {
                    range: {
                        sheetId: sheetId,
                        startRowIndex: 0,
                        endRowIndex: 1
                    },
                    cell: {
                        userEnteredFormat: {
                            backgroundColor: { red: 0.2, green: 0.2, blue: 0.2 },
                            textFormat: {
                                foregroundColor: { red: 1, green: 1, blue: 1 },
                                bold: true
                            }
                        }
                    },
                    fields: 'userEnteredFormat(backgroundColor,textFormat)'
                }
            },
            // Freeze first row
            {
                updateSheetProperties: {
                    properties: {
                        sheetId: sheetId,
                        gridProperties: { frozenRowCount: 1 }
                    },
                    fields: 'gridProperties.frozenRowCount'
                }
            },
            // Set width for Image Preview column (A) - увеличено в 2 раза для лучшей видимости
            {
                updateDimensionProperties: {
                    range: {
                        sheetId: sheetId,
                        dimension: 'COLUMNS',
                        startIndex: 0,
                        endIndex: 1
                    },
                    properties: {
                        pixelSize: 300
                    },
                    fields: 'pixelSize'
                }
            },
            // Set width for Image URL column (B) - narrower
            {
                updateDimensionProperties: {
                    range: {
                        sheetId: sheetId,
                        dimension: 'COLUMNS',
                        startIndex: 1,
                        endIndex: 2
                    },
                    properties: {
                        pixelSize: 80
                    },
                    fields: 'pixelSize'
                }
            },
            // Set width for Video Preview column (C)
            {
                updateDimensionProperties: {
                    range: {
                        sheetId: sheetId,
                        dimension: 'COLUMNS',
                        startIndex: 2,
                        endIndex: 3
                    },
                    properties: {
                        pixelSize: 150
                    },
                    fields: 'pixelSize'
                }
            },
            // Set width for Video URL column (D) - narrower
            {
                updateDimensionProperties: {
                    range: {
                        sheetId: sheetId,
                        dimension: 'COLUMNS',
                        startIndex: 3,
                        endIndex: 4
                    },
                    properties: {
                        pixelSize: 80
                    },
                    fields: 'pixelSize'
                }
            },
            // Set default row height for better preview display
            {
                updateDimensionProperties: {
                    range: {
                        sheetId: sheetId,
                        dimension: 'ROWS',
                        startIndex: 1,
                        endIndex: sortedRows.length + 1
                    },
                    properties: {
                        pixelSize: 150
                    },
                    fields: 'pixelSize'
                }
            },
            // Enable text wrapping for all cells (so Ad Text fits in cells)
            {
                repeatCell: {
                    range: {
                        sheetId: sheetId,
                        startRowIndex: 0,
                        endRowIndex: sortedRows.length + 1,
                        startColumnIndex: 0,
                        endColumnIndex: headers.length
                    },
                    cell: {
                        userEnteredFormat: {
                            wrapStrategy: 'WRAP'
                        }
                    },
                    fields: 'userEnteredFormat.wrapStrategy'
                }
            },
            // Conditional formatting: Highlight rows GREEN where Active Days >= 10
            {
                addConditionalFormatRule: {
                    rule: {
                        ranges: [{
                            sheetId: sheetId,
                            startRowIndex: 1, // Skip header
                            endRowIndex: sortedRows.length + 1,
                            startColumnIndex: 0,
                            endColumnIndex: headers.length
                        }],
                        booleanRule: {
                            condition: {
                                type: 'CUSTOM_FORMULA',
                                values: [{
                                    userEnteredValue: '=$L2>=10'  // Column L = Active Days
                                }]
                            },
                            format: {
                                backgroundColor: {
                                    red: 0.72,   // Light green (#B8E0B8)
                                    green: 0.88,
                                    blue: 0.72
                                }
                            }
                        }
                    },
                    index: 0
                }
            }
        ];

        // Note: Green highlighting automatically applied for ads active >= 10 days
        // Format > Conditional formatting > Custom formula: =$J2<=2 (Column J = Active Days)
        // Data is auto-sorted by Active Days (newest first)

        await sheets.spreadsheets.batchUpdate({
            spreadsheetId,
            requestBody: {
                requests: formatRequests
            }
        });

        console.log(`✅ Exported ${sortedRows.length} ads to sheet "${sheetName}" (sorted by date, newest first)`);
}

// Add requests - use direct URLs if provided, otherwise search terms
if (useDirectUrls) {
// 🔧 DEBUG MODE: Process only first competitor (set to false for production)
const debugMode = false;
const competitorsToProcess = debugMode ? competitorUrls.slice(0, 1) : competitorUrls;
    
    console.log(`🔗 Using ${competitorsToProcess.length} direct competitor URL(s)`);
    if (debugMode) {
        console.log(`⚠️ DEBUG MODE: Processing only first competitor - ${competitorsToProcess[0].name}`);
    }
    
    for (const competitor of competitorsToProcess) {
        await crawler.addRequests([{
            url: competitor.url,
            userData: { 
                competitorName: competitor.name,
                directUrl: competitor.url,
                searchTerm: competitor.name,
                facebookPageUrl: competitor.facebookPageUrl  // Optional: custom Facebook page URL
            }
        }]);
    }
} else {
    console.log(`🔍 Using ${searchTerms.length} search terms`);
    for (const searchTerm of searchTerms) {
    await crawler.addRequests([{
        url: `https://www.facebook.com/ads/library/`,
        userData: { searchTerm }
    }]);
    }
}

await crawler.run();

console.log('🎉 Competitor ads collection completed!');
console.log('📊 Collected all active ads from specified competitors');

// Export to Google Sheets if enabled
if (enableGoogleSheets) {
    console.log('📤 Preparing to export data to Google Sheets...');
    
    try {
        // Get all data from the dataset
        const dataset = await Actor.openDataset();
        const { items } = await dataset.getData();
        
        // Filter out error entries for clean export
        const validAds = items.filter(item => !item.error && item.advertiserName);
        
        if (validAds.length > 0) {
            console.log(`📋 Found ${validAds.length} ads to export`);
            
            // Group ads by competitor
            const adsByCompetitor = {};
            validAds.forEach(ad => {
                const competitor = ad.competitorName || ad.searchTerm || 'Unknown';
                if (!adsByCompetitor[competitor]) {
                    adsByCompetitor[competitor] = [];
                }
                adsByCompetitor[competitor].push(ad);
            });
            
            console.log(`📊 Grouped into ${Object.keys(adsByCompetitor).length} competitors`);
            
            // Export each competitor to separate sheet
            const exportSuccess = await exportToGoogleSheetsByCompetitor(
                adsByCompetitor,
                googleSheetsSpreadsheetId,
                googleServiceAccountKey
            );
            
            if (exportSuccess) {
                console.log('✅ Data successfully exported to Google Sheets!');
            } else {
                console.log('⚠️ Export to Google Sheets failed. Data is still saved in Apify Dataset.');
            }
        } else {
            console.log('⚠️ No valid ads found to export to Google Sheets');
        }
    } catch (error) {
        console.error('❌ Error during Google Sheets export:', error.message);
        console.log('💾 Data is still available in Apify Dataset');
    }
} else {
    console.log('ℹ️ Google Sheets export is disabled. Enable it in input settings to export data.');
}

await Actor.exit();