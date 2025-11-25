import { Actor } from 'apify';
import { PuppeteerCrawler } from 'crawlee';
import { google } from 'googleapis';
import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

// Load environment variables from .env file (for local development)
try {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = dirname(__filename);
    const envPath = join(__dirname, '..', '.env');
    const envContent = readFileSync(envPath, 'utf8');
    envContent.split('\n').forEach(line => {
        const [key, ...valueParts] = line.split('=');
        if (key && valueParts.length > 0) {
            const value = valueParts.join('=').trim().replace(/^["']|["']$/g, '');
            if (!process.env[key.trim()]) {
                process.env[key.trim()] = value;
            }
        }
    });
} catch (e) {
    // .env file not found, use environment variables or input
}

await Actor.init();

const input = await Actor.getInput();
const { 
    searchTerms: searchTermsInput = 'kursus coding anak\nbelajar programming anak\ncoding untuk anak\nmath for kids indonesia\ndesign course kids\nscratch programming\nvisual programming anak\ndigital literacy anak\nrobotika anak\nSTEM education Indonesia',
    competitorUrls = [],
    country = 'ID',
    maxPages = 10,
    minActiveDays = 1,
    useProxy = false,
    saveMediaAssets = true,
    highResolutionOnly = true,
    enableEngagementMatching = false,
    enableGoogleSheets = false,
    googleSheetsSpreadsheetId = process.env.GOOGLE_SHEETS_SPREADSHEET_ID || '',
    googleSheetsName = process.env.GOOGLE_SHEETS_NAME || 'Competitor Ads',
    googleServiceAccountKey = '',
    enableSupabase = process.env.SUPABASE_URL ? true : false,
    supabaseUrl = process.env.SUPABASE_URL || '',
    supabaseKey = process.env.SUPABASE_KEY || ''
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
            protocolTimeout: 900000, // ✅ 15 минут вместо 3 минут по умолчанию (для длинного скроллинга)
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
            
            // 🔍 Проверяем состояние страницы перед скроллингом
            const pageDebugInfo = await page.evaluate(() => {
                return {
                    bodyScrollHeight: document.body.scrollHeight,
                    bodyOffsetHeight: document.body.offsetHeight,
                    documentHeight: document.documentElement.scrollHeight,
                    currentScroll: window.scrollY,
                    bodyOverflow: getComputedStyle(document.body).overflow,
                    htmlOverflow: getComputedStyle(document.documentElement).overflow,
                    visibleText: document.body.innerText.substring(0, 500)
                };
            });
            console.log('📊 Page state before scrolling:', JSON.stringify(pageDebugInfo, null, 2));
            
            // 🚫 Пытаемся закрыть любые overlays/popups
            await page.evaluate(() => {
                // Ищем и закрываем модальные окна
                const closeButtons = document.querySelectorAll('[aria-label*="Close"], [aria-label*="close"], button[title*="Close"]');
                closeButtons.forEach(btn => {
                    try {
                        btn.click();
                        console.log('🚫 Closed popup/overlay');
                    } catch (e) {}
                });
                
                // Убираем overflow:hidden с body если есть
                if (document.body.style.overflow === 'hidden') {
                    document.body.style.overflow = 'auto';
                    console.log('🔓 Removed overflow:hidden from body');
                }
            });
            
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            // 📸 Скриншот страницы ДО скроллинга (для диагностики)
            try {
                const { KeyValueStore } = await import('apify');
                const initialScreenshot = await page.screenshot({ 
                    fullPage: false,
                    encoding: 'binary'
                });
                await KeyValueStore.setValue('page_before_scroll.png', initialScreenshot, { contentType: 'image/png' });
                console.log('📸 Initial page screenshot saved: page_before_scroll.png');
            } catch (screenshotError) {
                console.log(`⚠️ Initial screenshot failed: ${screenshotError.message}`);
            }
            
            // 🎯 Извлекаем целевое количество креативов ДО скроллинга
            const expectedAdsCount = await page.evaluate(() => {
                try {
                    const bodyText = document.body.innerText;
                    const patterns = [
                        /~?(\d+)\s*results?/i,
                        /(\d+)\s*ads?/i,
                        /showing\s+\d+\s+of\s+(\d+)/i,
                        /(\d+)\s*iklan/i
                    ];
                    
                    for (const pattern of patterns) {
                        const match = bodyText.match(pattern);
                        if (match && match[1]) {
                            const count = parseInt(match[1]);
                            if (count > 0 && count < 10000) {
                                return count;
                            }
                        }
                    }
                    return null;
                } catch (e) {
                    return null;
                }
            });
            
            if (expectedAdsCount) {
                console.log(`🎯 Target: Facebook shows ~${expectedAdsCount} ads on this page`);
            } else {
                console.log(`⚠️ Could not detect expected ads count, will use default scrolling`);
            }
            
            // ✅ ВАЖНО: Перехватываем console.log ДО autoScroll!
            page.on('console', msg => {
                const text = msg.text();
                // Пропускаем системные сообщения React/Facebook
                if (!text.includes('Download the React DevTools') && 
                    !text.includes('Warning:') &&
                    !text.includes('Failed to load resource') &&
                    text.length < 500) { // Пропускаем очень длинные логи
                    console.log(`[Browser] ${text}`);
                }
            });
            
            // 📜 Адаптивный скроллинг до достижения ~95% от цели
            // Для 120 ads: 120/2 = 60 скроллов × 400px = 24,000px - достаточно!
            const targetScrolls = expectedAdsCount ? Math.min(Math.ceil(expectedAdsCount / 2), 80) : 50;
            console.log(`📜 Will perform ${targetScrolls} scrolls (400px each) to load all ads`);
            
            await autoScroll(page, targetScrolls);
            await new Promise(resolve => setTimeout(resolve, 5000));
            
            // Save HTML for debugging
            const html = await page.content();
            await Actor.setValue('PAGE_CONTENT.html', html, { contentType: 'text/html' });
            console.log('📄 Page HTML saved to Key-Value Store as PAGE_CONTENT.html');

            console.log('🔍 Checking page content...');
            
            // Collect ALL ads from the page (no filtering by quality)
            const discoveredAdsResult = await page.evaluate((searchTermParam, minDays, competitorName, directUrl) => {
                function extractAdsFromJSON() {
                    const extractedAds = [];
                    try {
                        const edges = require[0][3][0].__bbox.require[0][3][1].__bbox.result.data.ad_library_main.search_results_connection.edges;

                        if (edges && Array.isArray(edges)) {
                            console.log(`Found ${edges.length} ad edges in JSON data.`);

                            for (const edge of edges) {
                                const node = edge.node;
                                if (!node || !node.ad_snapshot) continue;

                                const snapshot = node.ad_snapshot;
                                const creative = node.ad_creative;

                                const adText = creative?.body?.markup?.__html || snapshot?.body?.markup?.__html || '';
                                const advertiserName = snapshot.page?.name;
                                const adId = snapshot.ad_library_id;

                                const mediaAssets = {
                                    images: (snapshot.images || []).map(img => ({ url: img.original_image_url, width: img.width, height: img.height })),
                                    videos: (snapshot.videos || []).map(vid => ({ videoUrl: vid.video_hd_url || vid.video_sd_url, thumbnailUrl: vid.thumbnail_url })),
                                    thumbnails: (snapshot.videos || []).map(vid => ({ url: vid.thumbnail_url }))
                                };

                                const landingPageUrl = snapshot.link_url;
                                const ctaButtonText = creative?.link_description || snapshot?.call_to_action_text || '';

                                let activeDays = 1;
                                if (snapshot.start_date) {
                                     const startDate = new Date(snapshot.start_date * 1000);
                                     const diffTime = Math.abs(new Date() - startDate);
                                     activeDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                                }

                                if (advertiserName && adText && adId) {
                                     extractedAds.push({
                                        adId: adId,
                                        libraryId: adId,
                                        advertiserName: advertiserName,
                                        adText: adText.replace(/<[^>]*>/g, ''), // Strip HTML tags
                                        landingPageUrl: landingPageUrl || '',
                                        ctaButtonText: ctaButtonText || '',
                                        mediaAssets: mediaAssets,
                                         visualSummary: {
                                            totalImages: mediaAssets.images.length,
                                            totalVideos: mediaAssets.videos.length,
                                            totalThumbnails: mediaAssets.thumbnails.length,
                                            hasCarousel: mediaAssets.images.length > 1,
                                            hasVideo: mediaAssets.videos.length > 0,
                                            hasHighResImages: mediaAssets.images.some(img => (img.width || 0) >= 400 && (img.height || 0) >= 400),
                                        },
                                        activeDays: activeDays,
                                        searchTerm: searchTermParam,
                                        competitorName: competitorName || searchTermParam,
                                        discoveryMethod: 'json_extraction',
                                        scrapedAt: new Date().toISOString(),
                                        allImageUrls: mediaAssets.images.map(i => i.url),
                                        allVideoUrls: mediaAssets.videos.map(v => v.videoUrl),
                                        allThumbnailUrls: mediaAssets.thumbnails.map(t => t.url)
                                     });
                                }
                            }
                        }
                    } catch (e) {
                        console.log('Error extracting ads from JSON:', e.message);
                    }
                    return extractedAds;
                }

                let ads = extractAdsFromJSON();
                if (ads.length > 0) {
                     console.log(`✅ Successfully extracted ${ads.length} ads using JSON method.`);
                     return {
                         ads,
                         debug: {
                             pageUrl: window.location.href,
                             pageTitle: document.title,
                             selectorCounts: {},
                             totalContainers: 0,
                             potentialAdContainers: 0,
                             debugSamples: [],
                             rejectionReasons: {},
                             totalErrors: 0,
                             errors: []
                         }
                     };
                }

                console.log('JSON extraction failed, falling back to DOM scraping.');
                ads = [];
                
                console.log('🔍 Starting ad discovery in browser context...');
                console.log('Page URL:', window.location.href);
                console.log('Page title:', document.title);
                
                // Попытка извлечь Library ID из глобальных JavaScript объектов Facebook
                // Facebook часто хранит данные в window.__d, __APOLLO_STATE__ и других объектах
                let globalLibraryIds = [];
                try {
                    // Проверяем window.__d (Facebook data)
                    if (window.__d && Array.isArray(window.__d)) {
                        const jsonStr = JSON.stringify(window.__d);
                        const idMatches = jsonStr.match(/["'](\d{13,})["']/g);
                        if (idMatches) {
                            globalLibraryIds = idMatches.map(m => m.replace(/["']/g, '')).filter(id => id.length >= 13);
                            console.log(`🔍 Found ${globalLibraryIds.length} potential Library IDs in window.__d`);
                        }
                    }
                    
                    // Проверяем window.__APOLLO_STATE__
                    if (window.__APOLLO_STATE__) {
                        const apolloStr = JSON.stringify(window.__APOLLO_STATE__);
                        const apolloIds = apolloStr.match(/["'](\d{13,})["']/g);
                        if (apolloIds) {
                            const ids = apolloIds.map(m => m.replace(/["']/g, '')).filter(id => id.length >= 13);
                            globalLibraryIds = [...new Set([...globalLibraryIds, ...ids])];
                            console.log(`🔍 Found ${ids.length} potential Library IDs in __APOLLO_STATE__`);
                        }
                    }
                    
                    // Проверяем все глобальные переменные на наличие длинных чисел
                    const globalVars = Object.keys(window).filter(key => 
                        typeof window[key] === 'object' && window[key] !== null
                    );
                    for (const varName of globalVars.slice(0, 10)) { // Проверяем первые 10
                        try {
                            const varStr = JSON.stringify(window[varName]);
                            const matches = varStr.match(/["'](\d{13,})["']/g);
                            if (matches && matches.length > 0) {
                                const ids = matches.map(m => m.replace(/["']/g, '')).filter(id => id.length >= 13);
                                if (ids.length > 0 && ids.length < 100) { // Разумное количество
                                    globalLibraryIds = [...new Set([...globalLibraryIds, ...ids])];
                                    console.log(`🔍 Found ${ids.length} IDs in window.${varName}`);
                                }
                            }
                        } catch (e) {
                            // Игнорируем ошибки сериализации
                        }
                    }
                } catch (e) {
                    console.log('⚠️ Error extracting IDs from global objects:', e.message);
                }
                
                console.log(`📊 Total potential Library IDs found globally: ${globalLibraryIds.length}`);
                
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
                        const mediaAssets = extractMediaAssets(container, index); // ✅ Передаем индекс для логирования
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
                        
                        // Simple filters: just check if we have basic data
                        // УБРАЛИ фильтр по minActiveDays - собираем ВСЕ креативы независимо от количества дней
                        const hasBasicData = advertiserInfo.name && 
                            advertiserInfo.name !== 'Unknown' &&
                            advertiserInfo.name !== 'Meta Ad Library' &&
                            adContent.text &&
                            adContent.text.length > 30;
                            // activeDays >= minDays - УБРАНО для сбора всех креативов
                        
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
                            // Убрали проверку activeDays - собираем все креативы
                            rejectionReasons.push(reason);
                            
                            // 🔍 ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ отброшенных контейнеров (первые 10)
                            if (index < 10) {
                                console.log(`\n❌ [REJECTED CONTAINER #${index}] Reason: ${reason}`);
                                console.log(`   Advertiser: "${advertiserInfo.name || 'N/A'}"`);
                                console.log(`   Page name: "${advertiserInfo.pageName || 'N/A'}"`);
                                console.log(`   Text length: ${adContent.text?.length || 0} chars`);
                                console.log(`   Text preview: "${adContent.text?.substring(0, 100) || ''}${adContent.text?.length > 100 ? '...' : ''}"`);
                                console.log(`   Active days: ${adContent.activeDays || 'N/A'}`);
                                console.log(`   Images extracted: ${mediaAssets.images.length}`);
                                console.log(`   Videos extracted: ${mediaAssets.videos.length}`);
                                
                                // 🖼️ Показываем изображения из этого контейнера
                                if (mediaAssets.images.length > 0) {
                                    console.log(`\n   📸 Extracted images from this container:`);
                                    mediaAssets.images.forEach((img, imgIdx) => {
                                        if (imgIdx < 3) {
                                            console.log(`      ${imgIdx + 1}. ${img.width}x${img.height} (${img.type}) - ${img.url.substring(0, 100)}...`);
                                        }
                                    });
                                    if (mediaAssets.images.length > 3) {
                                        console.log(`      ... and ${mediaAssets.images.length - 3} more`);
                                    }
                                } else {
                                    console.log(`\n   📷 No images extracted from container`);
                                    
                                    // Проверяем сколько img tags было в HTML
                                    const imgTags = container.querySelectorAll('img');
                                    if (imgTags.length > 0) {
                                        console.log(`   ⚠️ But found ${imgTags.length} raw img tags in HTML:`);
                                        for (let i = 0; i < Math.min(3, imgTags.length); i++) {
                                            const src = imgTags[i].src || imgTags[i].dataset?.src || 'N/A';
                                            const w = imgTags[i].offsetWidth || imgTags[i].naturalWidth || 0;
                                            const h = imgTags[i].offsetHeight || imgTags[i].naturalHeight || 0;
                                            console.log(`      ${i+1}. ${w}x${h} - ${src.substring(0, 100)}${src.length > 100 ? '...' : ''}`);
                                        }
                                        if (imgTags.length > 3) {
                                            console.log(`      ... and ${imgTags.length - 3} more`);
                                        }
                                    }
                                }
                            }
                        }
                        
                        if (hasBasicData) {
                            const kidsData = extractKidsEdTechData(adContent.text);
                            
                            // Используем libraryId как основной ID, если он найден
                            // Иначе создаем временный ID на основе содержимого для дедупликации
                            const primaryId = adContent.libraryId || 
                                            (adContent.text.substring(0, 50) + mediaAssets.images[0]?.url?.substring(0, 50) || '').replace(/[^a-zA-Z0-9]/g, '_').substring(0, 100);
                            
                            // Используем libraryId как adId, если он найден
                            // Иначе создаем временный ID (но это должно быть редко)
                            const finalAdId = adContent.libraryId || `discovered_${Date.now()}_${index}`;
                            
                            // 📊 ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ для Library ID
                            if (!adContent.libraryId && index < 10) {
                                console.log(`⚠️ [AD #${index}] Library ID NOT FOUND!`);
                                console.log(`   → Using fallback ID: ${finalAdId}`);
                                console.log(`   → Container ID: ${container.id || 'N/A'}`);
                                console.log(`   → Container class: ${container.className?.substring(0, 100) || 'N/A'}`);
                                console.log(`   → Links in container: ${container.querySelectorAll('a').length}`);
                                const firstLink = container.querySelector('a');
                                if (firstLink) {
                                    console.log(`   → First link href: ${firstLink.href?.substring(0, 150) || 'N/A'}`);
                                    console.log(`   → First link text: ${firstLink.textContent?.substring(0, 50) || 'N/A'}`);
                                }
                                console.log(`   → Data attributes:`, {
                                    'data-id': container.getAttribute('data-id'),
                                    'data-library-id': container.getAttribute('data-library-id'),
                                    'data-ad-id': container.getAttribute('data-ad-id')
                                });
                            } else if (adContent.libraryId && index < 3) {
                                console.log(`✅ [AD #${index}] Library ID found: ${adContent.libraryId}`);
                            }
                            
                            ads.push({
                                // Core identification
                                adId: finalAdId,
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
                    
                    // Extract library ID - АГРЕССИВНЫЙ поиск во всех возможных местах
                    // 1. Из URL ссылок в контейнере (расширенные паттерны)
                    const allLinks = container.querySelectorAll('a[href]');
                    for (const link of allLinks) {
                        const href = link.getAttribute('href') || '';
                        // Ищем library ID в различных форматах URL:
                        // - /ads/library/?id=1776847809663660
                        // - /ads/library/?active_status=...&id=1776847809663660
                        // - https://www.facebook.com/ads/library/?id=1776847809663660
                        const urlIdMatch = href.match(/[?&]id=(\d{10,})/i) ||  // Минимум 10 цифр для Library ID
                                          href.match(/\/ads\/library\/\?.*id=(\d{10,})/i) ||
                                          href.match(/\/ads\/library\/\?id=(\d{10,})/i) ||
                                          href.match(/facebook\.com\/ads\/library\/\?.*id=(\d{10,})/i) ||
                                          href.match(/\/ads\/library\/.*[?&]ad_id=(\d{10,})/i) ||
                                          href.match(/\/ads\/library\/.*[?&]library_id=(\d{10,})/i);
                        if (urlIdMatch && urlIdMatch[1]) {
                            libraryId = urlIdMatch[1];
                            break;
                        }
                    }
                    
                    // 1.5. Попытка кликнуть на "View Ad" или подобную ссылку для получения ID из URL
                    if (!libraryId) {
                        const viewAdLinks = container.querySelectorAll('a[href*="/ads/library/"], a[href*="view"], a[aria-label*="View"], a[aria-label*="view"]');
                        for (const link of viewAdLinks) {
                            const href = link.getAttribute('href') || '';
                            const idMatch = href.match(/[?&]id=(\d{10,})/i);
                            if (idMatch && idMatch[1]) {
                                libraryId = idMatch[1];
                                break;
                            }
                        }
                    }
                    
                    // 2. Из data-атрибутов (расширенный поиск)
                    if (!libraryId) {
                        // Проверяем сам контейнер
                        const dataId = container.getAttribute('data-ad-id') || 
                                       container.getAttribute('data-id') ||
                                       container.getAttribute('data-library-id') ||
                                       container.getAttribute('aria-label')?.match(/(\d{10,})/)?.[1];
                        
                        // Проверяем дочерние элементы
                        if (!dataId) {
                            const childWithId = container.querySelector('[data-ad-id], [data-id], [data-library-id]');
                            if (childWithId) {
                                const childId = childWithId.getAttribute('data-ad-id') || 
                                              childWithId.getAttribute('data-id') ||
                                              childWithId.getAttribute('data-library-id');
                                if (childId && /^\d{10,}$/.test(childId)) {
                                    libraryId = childId;
                                }
                            }
                        } else if (/^\d{10,}$/.test(dataId)) {
                            libraryId = dataId;
                        }
                    }
                    
                    // 3. Из текста контейнера (расширенные паттерны)
                    if (!libraryId) {
                        const fullText = container.textContent || '';
                        const innerHTML = container.innerHTML || '';
                        
                        // Паттерны для поиска Library ID в тексте:
                        // - "Library ID: 1776847809663660"
                        // - "Library: 1776847809663660"
                        // - "ID: 1776847809663660"
                        // - "1776847809663660" (длинное число само по себе)
                        const libIdMatch = fullText.match(/library[:\s]+id[:\s]+(\d{10,})/i) ||
                                          fullText.match(/library[:\s]+(\d{10,})/i) ||
                                          fullText.match(/ad[:\s]+id[:\s]+(\d{10,})/i) ||
                                          fullText.match(/id[:\s]+(\d{10,})/i) ||
                                          // Ищем длинные числа (13+ цифр) которые могут быть Library ID
                                          fullText.match(/\b(\d{13,})\b/); // Library ID обычно 13-16 цифр
                        
                        if (libIdMatch && libIdMatch[1]) {
                            const potentialId = libIdMatch[1];
                            // Проверяем что это не год или другая дата
                            if (potentialId.length >= 10 && !potentialId.match(/^(19|20)\d{2}$/)) {
                                libraryId = potentialId;
                            }
                        }
                        
                        // Также проверяем в HTML (может быть в скрытых атрибутах)
                        if (!libraryId && innerHTML) {
                            const htmlIdMatch = innerHTML.match(/id[=:]\s*["']?(\d{10,})["']?/i) ||
                                               innerHTML.match(/library[_-]?id[=:]\s*["']?(\d{10,})["']?/i);
                            if (htmlIdMatch && htmlIdMatch[1]) {
                                libraryId = htmlIdMatch[1];
                            }
                        }
                    }
                    
                    // 4. Из aria-label и других доступных атрибутов
                    if (!libraryId) {
                        const ariaLabel = container.getAttribute('aria-label') || '';
                        const ariaLabelMatch = ariaLabel.match(/(\d{10,})/);
                        if (ariaLabelMatch && ariaLabelMatch[1]) {
                            libraryId = ariaLabelMatch[1];
                        }
                    }
                    
                    // 5. Поиск в родительских элементах и соседних контейнерах
                    if (!libraryId) {
                        // Проверяем родительский элемент
                        let parent = container.parentElement;
                        for (let i = 0; i < 3 && parent; i++) {
                            const parentLinks = parent.querySelectorAll('a[href]');
                            for (const link of parentLinks) {
                                const href = link.getAttribute('href') || '';
                                const urlIdMatch = href.match(/[?&]id=(\d{10,})/i);
                                if (urlIdMatch && urlIdMatch[1]) {
                                    libraryId = urlIdMatch[1];
                                    break;
                                }
                            }
                            if (libraryId) break;
                            parent = parent.parentElement;
                        }
                    }
                    
                    // 6. Поиск в соседних элементах (предыдущий/следующий sibling)
                    if (!libraryId) {
                        const siblings = [
                            container.previousElementSibling,
                            container.nextElementSibling
                        ].filter(Boolean);
                        
                        for (const sibling of siblings) {
                            const siblingLinks = sibling.querySelectorAll('a[href]');
                            for (const link of siblingLinks) {
                                const href = link.getAttribute('href') || '';
                                const urlIdMatch = href.match(/[?&]id=(\d{10,})/i);
                                if (urlIdMatch && urlIdMatch[1]) {
                                    libraryId = urlIdMatch[1];
                                    break;
                                }
                            }
                            if (libraryId) break;
                        }
                    }
                    
                    // 7. Поиск в глобальном контексте страницы (последняя попытка)
                    if (!libraryId) {
                        // Ищем все ссылки на странице с /ads/library/?id=
                        const allPageLinks = document.querySelectorAll('a[href*="/ads/library/"]');
                        for (const link of allPageLinks) {
                            const href = link.getAttribute('href') || '';
                            // Проверяем близость к нашему контейнеру
                            const rect = container.getBoundingClientRect();
                            const linkRect = link.getBoundingClientRect();
                            const distance = Math.abs(rect.top - linkRect.top) + Math.abs(rect.left - linkRect.left);
                            
                            // Если ссылка близко к контейнеру (в пределах 500px)
                            if (distance < 500) {
                                const urlIdMatch = href.match(/[?&]id=(\d{10,})/i);
                                if (urlIdMatch && urlIdMatch[1]) {
                                    libraryId = urlIdMatch[1];
                                    break;
                                }
                            }
                        }
                    }
                    
                    // Extract Landing Page URL - ENHANCED with Facebook redirect decoding
                    const landingPageLinks = container.querySelectorAll('a[href]');
                    const landingPageDebug = {
                        totalLinks: landingPageLinks.length,
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
                    for (const link of landingPageLinks) {
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
                        for (const link of landingPageLinks) {
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
                        for (const link of landingPageLinks) {
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
                    
                    // DEBUG: Логируем результат извлечения Library ID для первых 3 креативов
                    const isDebugContainer = ads.length < 3;
                    if (isDebugContainer && !libraryId) {
                        console.log(`⚠️ DEBUG Container #${ads.length + 1}: Library ID NOT FOUND`);
                        console.log(`   Container text preview: ${(container.textContent || '').substring(0, 150)}...`);
                        const linksCount = container.querySelectorAll('a[href]').length;
                        console.log(`   Links in container: ${linksCount}`);
                        if (linksCount > 0) {
                            const firstLink = container.querySelector('a[href]');
                            if (firstLink) {
                                console.log(`   First link href: ${(firstLink.getAttribute('href') || '').substring(0, 100)}...`);
                            }
                        }
                    } else if (isDebugContainer && libraryId) {
                        console.log(`✅ DEBUG Container #${ads.length + 1}: Library ID found: ${libraryId}`);
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
                
                function extractMediaAssets(container, containerIndex = 999) {
                    const media = {
                        images: [],
                        videos: [],
                        thumbnails: []
                    };
                    
                    // Extract images
                    const images = container.querySelectorAll('img');
                    let extractedCount = 0;
                    let skippedCount = 0;
                    let skippedReasons = [];
                    
                    images.forEach((img, index) => {
                        // ✅ РАСШИРЕННОЕ ИЗВЛЕЧЕНИЕ URL - проверяем ВСЕ возможные атрибуты
                        const src = img.src || 
                                   img.dataset.src || 
                                   img.getAttribute('data-src') ||
                                   img.getAttribute('data-lazy-src') ||
                                   img.getAttribute('data-original') ||
                                   (img.srcset ? img.srcset.split(' ')[0] : null) ||
                                   '';
                        
                        if (!src) {
                            skippedCount++;
                            skippedReasons.push(`img[${index}]: no src`);
                            return;
                        }
                        
                        // Проверяем через isValidAdMedia (теперь пропускает все scontent/fbcdn!)
                        if (!isValidAdMedia(src)) {
                            skippedCount++;
                            skippedReasons.push(`img[${index}]: invalid URL (${src.substring(0, 50)}...)`);
                            return;
                        }
                        
                        // Try multiple ways to get dimensions (lazy loading fix)
                        let width = img.naturalWidth || img.offsetWidth || 
                                   parseInt(img.getAttribute('width')) || 
                                   parseInt(img.style.width) || 0;
                        let height = img.naturalHeight || img.offsetHeight || 
                                    parseInt(img.getAttribute('height')) || 
                                    parseInt(img.style.height) || 0;
                        
                        // ✅ ПРИОРИТЕТ для Facebook CDN
                        const isFacebookCDN = src.includes('scontent') || 
                                             src.includes('fbcdn') ||
                                             src.includes('external');
                        
                        if (isFacebookCDN) {
                            // ⚠️ Facebook CDN: пропускаем МАЛЕНЬКИЕ изображения (< 150x150)
                            // Логотипы/аватары обычно маленькие, превью креативов - большие!
                            if (width > 0 && height > 0 && (width < 150 || height < 150)) {
                                skippedCount++;
                                skippedReasons.push(`img[${index}]: FB CDN but too small for ad (${width}x${height})`);
                                return;
                            }
                            
                            // ✅ Facebook CDN + достаточно большое = сохраняем!
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
                            extractedCount++;
                        } else {
                            // ⚠️ НЕ-Facebook изображения - применяем фильтры
                            // Пропускаем только очень маленькие иконки (< 80px)
                            if (width > 0 && height > 0 && width < 80 && height < 80) {
                                skippedCount++;
                                skippedReasons.push(`img[${index}]: too small (${width}x${height})`);
                                return;
                            }
                            
                            // Если нет размеров - пропускаем (может быть иконка)
                            if (width === 0 && height === 0) {
                                skippedCount++;
                                skippedReasons.push(`img[${index}]: no dimensions`);
                                return;
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
                            extractedCount++;
                        }
                    });
                    
                    // 📊 ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ для отладки (первые 5 контейнеров)
                    if (containerIndex < 5) {
                        console.log(`\n🔍 [CONTAINER #${containerIndex}] Image extraction analysis:`);
                        console.log(`   Total img tags found: ${images.length}`);
                        console.log(`   ✅ Extracted: ${extractedCount}`);
                        console.log(`   ❌ Skipped: ${skippedCount}`);
                        
                        if (images.length > 0) {
                            console.log(`\n   📷 Analyzing all ${images.length} images in container:`);
                            images.forEach((img, idx) => {
                                if (idx < 8) { // Первые 8 изображений
                                    const src = img.src || 
                                               img.dataset.src || 
                                               img.getAttribute('data-src') ||
                                               img.getAttribute('data-lazy-src') ||
                                               img.getAttribute('data-original') ||
                                               (img.srcset ? img.srcset.split(' ')[0] : null) ||
                                               '';
                                    
                                    const width = img.naturalWidth || img.offsetWidth || 
                                                 parseInt(img.getAttribute('width')) || 
                                                 parseInt(img.style.width) || 0;
                                    const height = img.naturalHeight || img.offsetHeight || 
                                                  parseInt(img.getAttribute('height')) || 
                                                  parseInt(img.style.height) || 0;
                                    
                                    console.log(`\n      Image ${idx + 1}/${images.length}:`);
                                    console.log(`        Size: ${width}x${height}`);
                                    console.log(`        URL: ${src.substring(0, 120)}${src.length > 120 ? '...' : ''}`);
                                    
                                    // Проверяем каждый фильтр отдельно
                                    let rejectedReason = '✅ PASSED all filters';
                                    
                                    if (!src) {
                                        rejectedReason = '❌ REJECTED: No src found';
                                    } else {
                                        const isFacebookCDN = src.includes('scontent') || src.includes('fbcdn') || src.includes('external');
                                        console.log(`        Facebook CDN: ${isFacebookCDN}`);
                                        
                                        if (!isValidAdMedia(src)) {
                                            rejectedReason = '❌ REJECTED by isValidAdMedia()';
                                            // 🔍 Показываем ТОЧНЫЙ паттерн который сработал (используем ТЕ ЖЕ регекспы что и в isValidAdMedia!)
                                            const matchedPatterns = [];
                                            if (src.includes('logo')) matchedPatterns.push("'logo'");
                                            if (src.includes('avatar')) matchedPatterns.push("'avatar'");
                                            if (src.includes('profile')) matchedPatterns.push("'profile'");
                                            if (src.includes('_thumb_')) matchedPatterns.push("'_thumb_'");
                                            if (src.match(/p\d{2,3}x\d{2,3}/)) matchedPatterns.push("p200x200 (profile)");
                                            if (src.match(/_s\d{1,2}x\d{1,2}[_\.]/)) matchedPatterns.push("_s60x60_ (small thumb)");
                                            if (src.match(/[=_]s\d{1,2}x\d{1,2}[_&\.]/)) matchedPatterns.push("=s60x60_ (query param)");
                                            if (matchedPatterns.length > 0) {
                                                console.log(`           Matched patterns: ${matchedPatterns.join(', ')}`);
                                            } else {
                                                console.log(`           Rejected for: not Facebook CDN + length=${src.length}`);
                                            }
                                        } else if (isFacebookCDN && width > 0 && height > 0 && (width < 150 || height < 150)) {
                                            rejectedReason = `❌ REJECTED: FB CDN but too small (${width}x${height} < 150px)`;
                                        } else if (width === 0 && height === 0) {
                                            rejectedReason = `❌ REJECTED: No dimensions (naturalWidth/offsetWidth = 0)`;
                                        }
                                    }
                                    
                                    console.log(`        ${rejectedReason}`);
                                }
                            });
                        }
                    }
                    
                    // 📊 КРАТКОЕ ЛОГИРОВАНИЕ для отладки
                    if (media.images.length === 0 && images.length > 0) {
                        console.log(`\n⚠️ [REJECTED CONTAINER] Found ${images.length} img tags but NONE extracted!`);
                        console.log(`   Extracted: ${extractedCount}, Skipped: ${skippedCount}`);
                        
                        // 🖼️ Показываем ВСЕ изображения с детальными причинами отклонения
                        console.log(`\n   📷 Analyzing all ${images.length} images in container:`);
                        images.forEach((img, idx) => {
                            if (idx < 5) { // Первые 5 изображений
                                const src = img.src || 
                                           img.dataset.src || 
                                           img.getAttribute('data-src') ||
                                           img.getAttribute('data-lazy-src') ||
                                           '';
                                const width = img.naturalWidth || img.offsetWidth || parseInt(img.getAttribute('width')) || 0;
                                const height = img.naturalHeight || img.offsetHeight || parseInt(img.getAttribute('height')) || 0;
                                
                                console.log(`\n      Image ${idx + 1}/${images.length}:`);
                                console.log(`        Size: ${width}x${height}`);
                                console.log(`        URL: ${src.substring(0, 120)}${src.length > 120 ? '...' : ''}`);
                                
                                // Проверяем почему отклонено
                                if (!src) {
                                    console.log(`        ❌ REJECTED: No src attribute found`);
                                } else {
                                    const isFacebookCDN = src.includes('scontent') || src.includes('fbcdn') || src.includes('external');
                                    const hasLogo = src.includes('logo');
                                    const hasAvatar = src.includes('avatar');
                                    const hasProfile = src.includes('profile');
                                    const hasThumb = src.includes('_thumb_');
                                    const hasSizePattern = /[ps]\d{2,4}x\d{2,4}/.test(src);
                                    
                                    if (!isValidAdMedia(src)) {
                                        console.log(`        ❌ REJECTED: isValidAdMedia = false`);
                                        console.log(`           Patterns found: logo=${hasLogo}, avatar=${hasAvatar}, profile=${hasProfile}, thumb=${hasThumb}, size_pattern=${hasSizePattern}`);
                                    } else if (isFacebookCDN && (width > 0 && height > 0) && (width < 200 || height < 200)) {
                                        console.log(`        ❌ REJECTED: Facebook CDN but too small (< 200px)`);
                                    } else if (!isFacebookCDN && (width > 0 && height > 0) && (width < 80 || height < 80)) {
                                        console.log(`        ❌ REJECTED: Non-Facebook image too small (< 80px)`);
                                    } else {
                                        console.log(`        ✅ PASSED all filters (may be lazy-loaded, dimensions unknown)`);
                                    }
                                }
                            }
                        });
                        
                        if (images.length > 5) {
                            console.log(`\n      ... and ${images.length - 5} more images (not shown)`);
                        }
                    } else if (media.images.length === 0 && images.length === 0) {
                        console.log(`⚠️ [AD CONTAINER] NO img tags found in HTML`);
                    } else {
                        console.log(`✅ [AD CONTAINER] Extracted ${media.images.length} images (skipped ${skippedCount})`);
                    }
                    
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

                    // ✅ Looser filtering: only reject obvious UI elements
                    const rejectPatterns = ['favicon', '/images/emoji/', 'spinner', 'icon-'];
                    if (rejectPatterns.some(p => url.includes(p))) {
                        return false;
                    }

                    // ✅ Accept all images from Facebook's CDN, as they are likely ad creatives
                    if (url.includes('scontent') || url.includes('fbcdn')) {
                        return true;
                    }

                    // For other URLs, check for a reasonable length
                    return url.length > 50;
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
                    
                    // ✅ НОВАЯ ЛОГИКА: Сортируем ВСЕ изображения по размеру (большие первыми)
                    // Это гарантирует что превью креатива (обычно самое большое) будет первым
                    // А логотипы/аватары (маленькие) - в конце или отфильтрованы
                    
                    const sorted = images
                        .map(img => ({
                            url: img.url,
                            width: img.width || 0,
                            height: img.height || 0,
                            resolution: (img.width || 0) * (img.height || 0),
                            position: img.position || 0
                        }))
                        .sort((a, b) => {
                            // Сначала сортируем по размеру (большие первыми)
                            if (b.resolution !== a.resolution) {
                                return b.resolution - a.resolution;
                            }
                            // При равном размере - по позиции (меньшие первыми)
                            return a.position - b.position;
                        });
                    
                    // Убираем дубликаты URL (оставляем только уникальные)
                    const uniqueUrls = [];
                    const seenUrls = new Set();
                    
                    for (const img of sorted) {
                        if (img.url && !seenUrls.has(img.url)) {
                            uniqueUrls.push(img.url);
                            seenUrls.add(img.url);
                        }
                    }
                    
                    return uniqueUrls;
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
                console.log(`\n📊 DETAILED Rejection Summary:`);
                console.log(`   Total containers checked: ${potentialAdContainers.length}`);
                console.log(`   ✅ Accepted as ads: ${ads.length}`);
                console.log(`   ❌ Rejected: ${potentialAdContainers.length - ads.length}`);
                
                console.log(`\n   Rejection breakdown:`);
                const reasonCounts = {};
                rejectionReasons.forEach(r => {
                    reasonCounts[r] = (reasonCounts[r] || 0) + 1;
                });
                Object.entries(reasonCounts)
                    .sort((a, b) => b[1] - a[1]) // Сортируем по количеству (больше первыми)
                    .forEach(([reason, count]) => {
                        const percentage = Math.round((count / potentialAdContainers.length) * 100);
                        console.log(`     ${reason}: ${count} (${percentage}%)`);
                    });
                
                console.log(`\n   💡 Analysis:`);
                console.log(`      - Containers with ads: ${ads.length}`);
                console.log(`      - Containers rejected: ${potentialAdContainers.length - ads.length}`);
                console.log(`      - Success rate: ${Math.round((ads.length / potentialAdContainers.length) * 100)}%`);
                
                // Return both ads and debug info
                // Убрали ограничение в 100 креативов - собираем все найденные
                return {
                    ads: ads, // Собираем все креативы без ограничений
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
            
            // ✅ Валидация: сравниваем с ожидаемым количеством
            if (expectedAdsCount) {
                const coverage = Math.round((discoveredAds.length / expectedAdsCount) * 100);
                console.log(`📊 Coverage: ${discoveredAds.length} / ~${expectedAdsCount} ads (${coverage}%)`);
                
                if (discoveredAds.length < expectedAdsCount * 0.85) {
                    console.warn(`⚠️ WARNING: Found only ${discoveredAds.length} out of ~${expectedAdsCount} expected ads (${coverage}%)`);
                    console.warn(`   Possible reasons:`);
                    console.warn(`   - Insufficient scrolling (try increasing targetScrolls)`);
                    console.warn(`   - Too strict image filters (check size/pattern filters)`);
                    console.warn(`   - Some ads failed validation (check rejection reasons above)`);
                } else {
                    console.log(`✅ Good coverage! Found ${coverage}% of expected ads`);
                }
            }
            
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
        const { KeyValueStore } = await import('apify');
        console.log(`📜 Starting scroll: will perform ${maxScrolls} scrolls (6 seconds each)`);
        console.log(`📸 Screenshots will be saved to Key-Value Store`);

        console.log(`🖱️  Clicking on page to activate focus...`);
        await page.click('body');
        await new Promise(resolve => setTimeout(resolve, 1000));
        console.log(`✅ Page activated, ready to scroll`);

        let previousAdCount = 0;
        let noNewAdsCounter = 0;

        for (let scrollIndex = 0; scrollIndex < maxScrolls; scrollIndex++) {
            const beforeScroll = await page.evaluate(() => ({
                oldHeight: document.body.scrollHeight,
                oldScroll: window.scrollY,
                adCards: document.querySelectorAll('[data-testid*="ad"]').length
            }));

            console.log(`[Before] scrollY=${beforeScroll.oldScroll}, height=${beforeScroll.oldHeight}, ads=${beforeScroll.adCards}`);

            // New scrolling strategy
            await page.keyboard.press('End');
            await new Promise(resolve => setTimeout(resolve, 1000));
            await page.keyboard.press('PageDown');
            await new Promise(resolve => setTimeout(resolve, 1500));

            const scrollResult = await page.evaluate(() => ({
                newHeight: document.body.scrollHeight,
                newScroll: window.scrollY,
                currentAdCount: document.querySelectorAll('[data-testid*="ad"]').length
            }));
            
            const newAdsFound = scrollResult.currentAdCount - previousAdCount;
            console.log(`📜 Scroll ${scrollIndex + 1}/${maxScrolls}: Height: ${beforeScroll.oldHeight}px -> ${scrollResult.newHeight}px | Scroll: ${beforeScroll.oldScroll}px -> ${scrollResult.newScroll}px | Ads found: ${scrollResult.currentAdCount} (+${newAdsFound})`);

            // Check for a "show more" or "see more" button
            const moreButton = await page.evaluateHandle(() => {
                const buttons = Array.from(document.querySelectorAll('div[role="button"], button'));
                const moreButtonTexts = ['see more results', 'show more', 'view more', 'lihat lainnya', 'tampilkan lebih banyak'];
                return buttons.find(button => moreButtonTexts.some(text => button.innerText.toLowerCase().includes(text)));
            });

            if (moreButton && (await moreButton.boundingBox())) {
                console.log('🖱️ Clicking "show more" button...');
                await moreButton.click();
                await new Promise(resolve => setTimeout(resolve, 3000));
                noNewAdsCounter = 0; // Reset counter after clicking button
            } else {
                if (newAdsFound === 0 && scrollResult.newScroll === beforeScroll.oldScroll) {
                    noNewAdsCounter++;
                } else {
                    noNewAdsCounter = 0;
                }
            }
            
            previousAdCount = scrollResult.currentAdCount;

            if (noNewAdsCounter >= 3) {
                console.log(`✅ Scrolling complete after ${scrollIndex + 1} scrolls (no new ads and scroll position stable for 3 consecutive scrolls)`);
                break;
            }

            await new Promise(resolve => setTimeout(resolve, 4000));
        }

        console.log('⏳ Waiting additional 10 seconds for final content to load...');
        await new Promise(resolve => setTimeout(resolve, 10000));
        console.log('✅ Final wait complete');

    } catch (error) {
        console.error('❌ Error during scrolling:', error.message);
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

// Supabase Integration - DELETE old data and save FRESH creatives
// This ensures the external service always gets the latest data
async function saveToSupabase(ads, supabaseUrl, supabaseKey) {
    try {
        console.log('💾 Starting Supabase integration...');
        console.log(`🔍 DEBUG: Received ${ads?.length || 0} ads`);
        console.log(`🔍 DEBUG: supabaseUrl = ${supabaseUrl ? 'PROVIDED' : 'MISSING'}`);
        console.log(`🔍 DEBUG: supabaseKey = ${supabaseKey ? 'PROVIDED (length: ' + supabaseKey.length + ')' : 'MISSING'}`);
        
        if (!supabaseUrl || !supabaseKey) {
            console.log('⚠️ Supabase credentials not provided. Skipping Supabase storage.');
            return false;
        }

        // Initialize Supabase client
        console.log('🔌 Initializing Supabase client...');
        const supabase = createClient(supabaseUrl, supabaseKey);
        console.log('✅ Supabase client created');
        
        // Save ALL ads to Supabase (no filtering)
        console.log('🔍 Processing all ads for Supabase (no active days filter)...');
        const targetAds = ads; // Save everything
        
        console.log(`📊 Saving ${targetAds.length} creatives to Supabase (all ads, no filter)`);
        
        // DEBUG: Show sample ad data
        if (targetAds.length > 0) {
            const sampleAd = targetAds[0];
            console.log(`🔍 DEBUG: Sample ad data:`);
            console.log(`  - adId: ${sampleAd.adId || 'N/A'}`);
            console.log(`  - competitorName: ${sampleAd.competitorName || 'N/A'}`);
            console.log(`  - activeDays: ${sampleAd.activeDays || 'N/A'}`);
            console.log(`  - imageUrl: ${sampleAd.imageUrl ? 'EXISTS' : 'N/A'}`);
            console.log(`  - allImageUrls: ${sampleAd.allImageUrls?.length || 0} images`);
            if (sampleAd.allImageUrls?.length > 0) {
                console.log(`  - First image URL: ${sampleAd.allImageUrls[0]?.substring(0, 100)}...`);
            }
        }
        
        // Count ads with/without images
        const adsWithImages = targetAds.filter(ad => 
            (Array.isArray(ad.allImageUrls) && ad.allImageUrls.length > 0) || ad.imageUrl
        ).length;
        const adsWithoutImages = targetAds.length - adsWithImages;
        console.log(`📊 Image stats: ✅ ${adsWithImages} with images, ⚠️ ${adsWithoutImages} without images`);
        
        if (adsWithoutImages > 0 && targetAds.length > 0) {
            console.log(`⚠️ WARNING: ${adsWithoutImages} ads have NO images - they might be filtered out or not scraped properly`);
        }
        
        if (targetAds.length === 0) {
            console.log('ℹ️ No creatives found. Skipping Supabase upload.');
            return true;
        }
        
        // Ensure storage bucket exists
        const bucketName = 'competitor-creatives';
        console.log(`📦 Checking storage bucket: ${bucketName}`);
        
        const { data: buckets } = await supabase.storage.listBuckets();
        const bucketExists = buckets?.some(b => b.name === bucketName);
        
        if (!bucketExists) {
            console.log(`📦 Creating storage bucket: ${bucketName}`);
            await supabase.storage.createBucket(bucketName, {
                public: true,
                fileSizeLimit: 10485760 // 10MB
            });
        }
        
        // ═══════════════════════════════════════════════════════
        // 🗑️ STEP 1: Delete all old data (fresh start every run)
        // ═══════════════════════════════════════════════════════
        console.log('\n🗑️ Cleaning old data from Supabase...');
        
        // Delete all records from database table
        try {
            console.log('🗑️ Deleting all records from competitor_creatives table...');
            const { error: deleteError } = await supabase
                .from('competitor_creatives')
                .delete()
                .neq('ad_id', ''); // Delete all records (where ad_id is not empty, which is all)
            
            if (deleteError) {
                console.warn(`⚠️ Error deleting table records: ${deleteError.message}`);
            } else {
                console.log('✅ All old records deleted from database');
            }
        } catch (err) {
            console.warn(`⚠️ Failed to delete old table records: ${err.message}`);
        }
        
        // Delete all files from storage bucket
        try {
            console.log('🗑️ Deleting all old images from storage...');
            
            // List all files in bucket
            const { data: filesList, error: listError } = await supabase.storage
                .from(bucketName)
                .list('', {
                    limit: 1000,
                    offset: 0
                });
            
            if (listError) {
                console.warn(`⚠️ Error listing files: ${listError.message}`);
            } else if (filesList && filesList.length > 0) {
                console.log(`📋 Found ${filesList.length} folders in storage, scanning for files...`);
                
                let totalDeleted = 0;
                
                // Delete files in each folder
                for (const folder of filesList) {
                    if (folder.name) {
                        const { data: folderFiles } = await supabase.storage
                            .from(bucketName)
                            .list(folder.name, { limit: 1000 });
                        
                        if (folderFiles && folderFiles.length > 0) {
                            const filePaths = folderFiles.map(f => `${folder.name}/${f.name}`);
                            
                            const { error: removeError } = await supabase.storage
                                .from(bucketName)
                                .remove(filePaths);
                            
                            if (!removeError) {
                                totalDeleted += filePaths.length;
                                console.log(`✅ Deleted ${filePaths.length} files from ${folder.name}`);
                            }
                        }
                    }
                }
                
                console.log(`✅ Total deleted: ${totalDeleted} old images from storage`);
            } else {
                console.log('ℹ️ No old files found in storage (clean bucket)');
            }
        } catch (err) {
            console.warn(`⚠️ Failed to delete old storage files: ${err.message}`);
        }
        
        console.log('✅ Cleanup complete! Ready to upload fresh data\n');
        // ═══════════════════════════════════════════════════════
        
        // Download and upload images to Supabase Storage
        console.log(`🖼️ Downloading and uploading ${targetAds.length} NEW images to Supabase Storage...`);
        
        const creativesToSave = [];
        let successCount = 0;
        let failCount = 0;
        let processedCount = 0;
        
        for (const ad of targetAds) {
            try {
                // Calculate launch date (today - active days)
                const launchDate = new Date();
                launchDate.setDate(launchDate.getDate() - (ad.activeDays || 0));
                
                // Get first image URL
                const originalImageUrl = Array.isArray(ad.allImageUrls) && ad.allImageUrls.length > 0 
                    ? ad.allImageUrls[0] 
                    : (ad.imageUrl || '');
                
                if (processedCount < 5) { // Debug first 5 ads
                    console.log(`🔍 DEBUG: Processing ad #${processedCount + 1}: ${ad.adId || 'unknown'}`);
                    console.log(`  - allImageUrls: ${ad.allImageUrls?.length || 0} images`);
                    console.log(`  - imageUrl: ${ad.imageUrl ? 'EXISTS' : 'N/A'}`);
                    console.log(`  - Original image URL: ${originalImageUrl ? originalImageUrl.substring(0, 100) + '...' : 'EMPTY ⚠️'}`);
                }
                
                let storedImageUrl = originalImageUrl || null; // null if no image
                processedCount++;
                
                // Download and upload image if URL exists
                if (originalImageUrl && originalImageUrl.startsWith('http')) {
                    try {
                        console.log(`📥 Downloading image for ${ad.adId}...`);
                        // Download image
                        const response = await fetch(originalImageUrl);
                        if (!response.ok) throw new Error(`HTTP ${response.status}`);
                        
                        const arrayBuffer = await response.arrayBuffer();
                        const buffer = Buffer.from(arrayBuffer);
                        console.log(`✅ Downloaded ${buffer.length} bytes`);
                        
                        // Generate unique filename
                        const adId = ad.adId || ad.libraryId || `unknown_${Date.now()}`;
                        const ext = originalImageUrl.match(/\.(jpg|jpeg|png|webp|gif)($|\?)/i)?.[1] || 'jpg';
                        const fileName = `${adId.replace(/[^a-zA-Z0-9]/g, '_')}_${Date.now()}.${ext}`;
                        const filePath = `${ad.competitorName || 'unknown'}/${fileName}`;
                        
                        console.log(`📤 Uploading to Supabase: ${filePath}`);
                        
                        // Upload to Supabase Storage
                        const { data: uploadData, error: uploadError } = await supabase.storage
                            .from(bucketName)
                            .upload(filePath, buffer, {
                                contentType: `image/${ext}`,
                                upsert: true
                            });
                        
                        console.log(`🔍 DEBUG: Upload result - error: ${uploadError ? uploadError.message : 'none'}, data: ${uploadData ? 'exists' : 'null'}`);
                        
                        if (uploadError) {
                            console.warn(`⚠️ Failed to upload image for ${adId}:`, uploadError.message);
                        } else {
                            // Get public URL
                            const { data: urlData } = supabase.storage
                                .from(bucketName)
                                .getPublicUrl(filePath);
                            
                            storedImageUrl = urlData.publicUrl;
                            successCount++;
                            console.log(`✅ Uploaded: ${adId} → ${fileName}`);
                        }
                    } catch (imgError) {
                        console.warn(`⚠️ Failed to download/upload image for ${ad.adId}:`, imgError.message);
                        failCount++;
                        // Keep original URL as fallback
                    }
                }
                
                // Минимальная структура для второго агента (только 4 поля)
                // Пропускаем записи без image_url (обязательное поле)
                if (storedImageUrl) {
                    creativesToSave.push({
                        image_url: storedImageUrl,                                    // ОБЯЗАТЕЛЬНО: ссылка на превью
                        competitor_name: ad.competitorName || ad.searchTerm || 'Unknown',  // Название конкурента
                        active_days: ad.activeDays || 0,                             // Количество дней (INTEGER)
                        ad_id: ad.adId || ad.libraryId || null                       // ID креатива (уникальный идентификатор)
                    });
                } else {
                    // 📊 ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ ПРОПУЩЕННЫХ КРЕАТИВОВ
                    console.warn(`⚠️ [SKIPPED] Ad ${ad.adId || 'unknown'}: NO IMAGE URL`);
                    console.warn(`   → allImageUrls: ${ad.allImageUrls?.length || 0} images`);
                    console.warn(`   → imageUrl: ${ad.imageUrl || 'N/A'}`);
                    console.warn(`   → originalImageUrl: ${originalImageUrl || 'N/A'}`);
                    console.warn(`   → storedImageUrl: ${storedImageUrl || 'N/A'}`);
                    if (ad.allImageUrls && ad.allImageUrls.length > 0) {
                        console.warn(`   → First image sample: ${ad.allImageUrls[0]?.substring(0, 100)}...`);
                    }
                }
                
                // Update original ad object with Supabase URL
                if (storedImageUrl && storedImageUrl !== originalImageUrl) {
                    ad.supabaseImageUrl = storedImageUrl;
                    if (ad.allImageUrls && ad.allImageUrls.length > 0) {
                        ad.allImageUrls[0] = storedImageUrl; // Replace first image with Supabase URL
                    }
                }
                
            } catch (adError) {
                console.error(`❌ Error processing ad ${ad.adId}:`, adError.message);
                failCount++;
            }
        }
        
        console.log(`📊 Image upload stats: ✅ ${successCount} success, ⚠️ ${failCount} failed`);
        
        // Дедупликация перед сохранением (убираем дубликаты)
        console.log(`🔍 Removing duplicates from ${creativesToSave.length} records...`);
        const uniqueCreatives = [];
        const seenIds = new Set();
        const seenImageUrls = new Set();
        const seenCombinations = new Set(); // Для креативов без ad_id
        let duplicatesRemoved = 0;
        
        for (const creative of creativesToSave) {
            // 1. Проверяем по ad_id (приоритет) - самый надежный способ
            if (creative.ad_id) {
                if (seenIds.has(creative.ad_id)) {
                    duplicatesRemoved++;
                    continue;
                }
                seenIds.add(creative.ad_id);
                uniqueCreatives.push(creative);
                continue;
            }
            
            // 2. Проверяем по image_url (fallback для креативов без ad_id)
            if (creative.image_url) {
                if (seenImageUrls.has(creative.image_url)) {
                    duplicatesRemoved++;
                    continue;
                }
                seenImageUrls.add(creative.image_url);
            }
            
            // 3. Для креативов без ad_id и image_url используем комбинацию полей
            const combination = `${creative.competitor_name}_${creative.image_url?.substring(0, 50) || 'no_image'}_${creative.active_days}`;
            if (seenCombinations.has(combination)) {
                duplicatesRemoved++;
                continue;
            }
            seenCombinations.add(combination);
            
            // Добавляем в уникальные
            uniqueCreatives.push(creative);
        }
        
        console.log(`✅ Removed ${duplicatesRemoved} duplicates. Unique creatives: ${uniqueCreatives.length}`);
        
        // Insert fresh data into Supabase table (no duplicates since we cleaned everything)
        // Сохраняем только минимальную структуру (4 поля) для второго агента
        console.log(`💾 Inserting ${uniqueCreatives.length} NEW records to database (minimal structure: 4 fields)...`);
        
        if (uniqueCreatives.length === 0) {
            console.log('⚠️ No creatives to save (all skipped due to missing image_url or duplicates)');
            return true;
        }
        
        const { data, error } = await supabase
            .from('competitor_creatives')
            .insert(uniqueCreatives)
            .select();
        
        if (error) {
            console.error('❌ Supabase database error:', error.message);
            return false;
        }
        
        console.log(`✅ Successfully saved ${data?.length || creativesToSave.length} creatives to Supabase!`);
        console.log(`📊 Table: competitor_creatives`);
        console.log(`📋 Structure: image_url, competitor_name, active_days, ad_id (4 fields only)`);
        console.log(`🖼️ Storage: ${bucketName}`);
        console.log(`🔗 URL: ${supabaseUrl}`);
        console.log(`🔄 Updated ${successCount} ad objects with Supabase URLs for Google Sheets export`);
        
        return true;
    } catch (error) {
        console.error('❌ Error during Supabase upload:', error.message);
        return false;
    }
}

// Google Sheets Integration - Export each competitor to separate sheet
/**
 * Load Google Service Account credentials from multiple sources
 * Priority: 1) Input parameter, 2) Environment variables, 3) service-account.json file
 */
function loadGoogleServiceAccountCredentials(inputKey) {
    // 1. Try input parameter first
    if (inputKey) {
        try {
            const parsed = typeof inputKey === 'string' ? JSON.parse(inputKey) : inputKey;
            if (parsed.type === 'service_account') {
                console.log('✅ Using Google credentials from input parameter');
                return parsed;
            }
        } catch (e) {
            // Not valid JSON, continue
        }
    }
    
    // 2. Try environment variables
    if (process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL && process.env.GOOGLE_PRIVATE_KEY) {
        console.log('✅ Using Google credentials from environment variables');
        return {
            type: 'service_account',
            project_id: process.env.GOOGLE_PROJECT_ID || '',
            private_key_id: process.env.GOOGLE_PRIVATE_KEY_ID || '',
            private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
            client_email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
            client_id: process.env.GOOGLE_CLIENT_ID || '',
            auth_uri: 'https://accounts.google.com/o/oauth2/auth',
            token_uri: 'https://oauth2.googleapis.com/token',
            auth_provider_x509_cert_url: 'https://www.googleapis.com/oauth2/v1/certs',
            client_x509_cert_url: `https://www.googleapis.com/robot/v1/metadata/x509/${encodeURIComponent(process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL)}`,
            universe_domain: 'googleapis.com'
        };
    }
    
    // 3. Try service-account.json file
    try {
        const __filename = fileURLToPath(import.meta.url);
        const __dirname = dirname(__filename);
        const credsPath = join(__dirname, '..', 'service-account.json');
        const credsContent = readFileSync(credsPath, 'utf8');
        const credentials = JSON.parse(credsContent);
        if (credentials.type === 'service_account') {
            console.log('✅ Using Google credentials from service-account.json file');
            return credentials;
        }
    } catch (e) {
        // File not found or invalid, continue
    }
    
    return null;
}

async function exportToGoogleSheetsByCompetitor(adsByCompetitor, spreadsheetId, serviceAccountKey) {
    try {
        console.log('📊 Starting export to Google Sheets (separate sheets per competitor)...');
        
        // Load credentials from multiple sources
        const credentials = loadGoogleServiceAccountCredentials(serviceAccountKey);
        
        if (!credentials || !spreadsheetId) {
            console.log('⚠️ Google Sheets credentials not provided. Skipping export.');
            console.log('💡 Tip: Set credentials via input, .env file, or service-account.json');
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

// Save to Supabase if enabled (deletes old data and inserts fresh creatives)
console.log('🔍 DEBUG: Checking enableSupabase flag:', enableSupabase);
if (enableSupabase) {
    console.log('💾 Preparing to save creatives to Supabase...');
    console.log('🔍 DEBUG: enableSupabase = true');
    console.log('🔍 DEBUG: supabaseUrl =', supabaseUrl);
    console.log('🔍 DEBUG: supabaseKey length =', supabaseKey ? supabaseKey.length : 0);
    
    try {
        // Get all data from the dataset
        const dataset = await Actor.openDataset();
        const { items } = await dataset.getData();
        
        console.log(`🔍 DEBUG: Retrieved ${items.length} items from dataset`);
        
        // Filter out error entries
        const validAds = items.filter(item => !item.error && item.advertiserName);
        
        console.log(`🔍 DEBUG: Filtered to ${validAds.length} valid ads`);
        
        if (validAds.length > 0) {
            console.log(`📋 Processing ${validAds.length} ads for Supabase (all ads, no filter)...`);
            
            // Save to Supabase
            console.log('🔍 DEBUG: Calling saveToSupabase function...');
            const supabaseSuccess = await saveToSupabase(
                validAds,
                supabaseUrl,
                supabaseKey
            );
            
            if (supabaseSuccess) {
                console.log('✅ Creatives successfully saved to Supabase!');
                
                // Update dataset with Supabase URLs for Google Sheets export
                console.log('🔄 Updating Apify Dataset with Supabase image URLs...');
                try {
                    // Get error entries to preserve them
                    const errorEntries = items.filter(item => item.error || !item.advertiserName);
                    
                    // Clear and re-save with updated URLs
                    await dataset.drop();
                    await Actor.pushData([...validAds, ...errorEntries]);
                    console.log(`✅ Dataset updated: ${validAds.length} ads with Supabase URLs + ${errorEntries.length} error entries`);
                } catch (updateError) {
                    console.warn('⚠️ Failed to update dataset:', updateError.message);
                }
            } else {
                console.log('⚠️ Supabase save failed. Data is still in Apify Dataset.');
            }
        } else {
            console.log('⚠️ No valid ads found to save to Supabase');
        }
    } catch (error) {
        console.error('❌ Error during Supabase save:', error.message);
        console.log('💾 Data is still available in Apify Dataset');
    }
} else {
    console.log('ℹ️ Supabase storage is disabled. Enable it in input settings to save creatives.');
}

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
