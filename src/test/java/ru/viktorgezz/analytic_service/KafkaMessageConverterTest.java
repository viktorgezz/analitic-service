package ru.viktorgezz.analytic_service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.viktorgezz.analytic_service.kafka.KafkaMessageConverter;
import ru.viktorgezz.analytic_service.model.Check;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KafkaMessageConverterTest {

    private KafkaMessageConverter converter;

    private static final String JSON_FULL = """
            [
              {
                "url": "https://github.com",
                "timestamp": "2025-09-20T01:46:19.339932+00:00",
                "success": true,
                "error": null,
                "response_time": 0.7221615314483643,
                "status_code": 200,
                "content_type": "text/html; charset=utf-8",
                "content_length": null,
                "headers": {
                  "Date": "Sat, 20 Sep 2025 01:46:22 GMT",
                  "Content-Type": "text/html; charset=utf-8",
                  "Vary": "X-PJAX, X-PJAX-Container, Turbo-Visit, Turbo-Frame, X-Requested-With, Accept-Language,Accept-Encoding, Accept, X-Requested-With",
                  "Content-Language": "en-US",
                  "Etag": "W/\\"8149b7c6f656cfd3a60dac8742e54240\\"",
                  "Cache-Control": "max-age=0, private, must-revalidate",
                  "Strict-Transport-Security": "max-age=31536000; includeSubdomains; preload",
                  "X-Frame-Options": "deny",
                  "X-Content-Type-Options": "nosniff",
                  "X-XSS-Protection": "0",
                  "Referrer-Policy": "origin-when-cross-origin, strict-origin-when-cross-origin",
                  "Content-Security-Policy": "default-src 'none'; base-uri 'self'; child-src github.githubassets.com github.com/assets-cdn/worker/ github.com/assets/ gist.github.com/assets-cdn/worker/; connect-src 'self' uploads.github.com www.githubstatus.com collector.github.com raw.githubusercontent.com api.github.com github-cloud.s3.amazonaws.com github-production-repository-file-5c1aeb.s3.amazonaws.com github-production-upload-manifest-file-7fdce7.s3.amazonaws.com github-production-user-asset-6210df.s3.amazonaws.com *.rel.tunnels.api.visualstudio.com wss://*.rel.tunnels.api.visualstudio.com github.githubassets.com objects-origin.githubusercontent.com copilot-proxy.githubusercontent.com proxy.individual.githubcopilot.com proxy.business.githubcopilot.com proxy.enterprise.githubcopilot.com *.actions.githubusercontent.com wss://*.actions.githubusercontent.com productionresultssa0.blob.core.windows.net/ productionresultssa1.blob.core.windows.net/ productionresultssa2.blob.core.windows.net/ productionresultssa3.blob.core.windows.net/ productionresultssa4.blob.core.windows.net/ productionresultssa5.blob.core.windows.net/ productionresultssa6.blob.core.windows.net/ productionresultssa7.blob.core.windows.net/ productionresultssa8.blob.core.windows.net/ productionresultssa9.blob.core.windows.net/ productionresultssa10.blob.core.windows.net/ productionresultssa11.blob.core.windows.net/ productionresultssa12.blob.core.windows.net/ productionresultssa13.blob.core.windows.net/ productionresultssa14.blob.core.windows.net/ productionresultssa15.blob.core.windows.net/ productionresultssa16.blob.core.windows.net/ productionresultssa17.blob.core.windows.net/ productionresultssa18.blob.core.windows.net/ productionresultssa19.blob.core.windows.net/ github-production-repository-image-32fea6.s3.amazonaws.com github-production-release-asset-2e65be.s3.amazonaws.com insights.github.com wss://alive.github.com wss://alive-staging.github.com api.githubcopilot.com api.individual.githubcopilot.com api.business.githubcopilot.com api.enterprise.githubcopilot.com edge.fullstory.com rs.fullstory.com; font-src github.githubassets.com; form-action 'self' github.com gist.github.com copilot-workspace.githubnext.com objects-origin.githubusercontent.com; frame-ancestors 'none'; frame-src viewscreen.githubusercontent.com notebooks.githubusercontent.com www.youtube-nocookie.com; img-src 'self' data: blob: github.githubassets.com media.githubusercontent.com camo.githubusercontent.com identicons.github.com avatars.githubusercontent.com private-avatars.githubusercontent.com github-cloud.s3.amazonaws.com objects.githubusercontent.com release-assets.githubusercontent.com secured-user-images.githubusercontent.com/ user-images.githubusercontent.com/ private-user-images.githubusercontent.com opengraph.githubassets.com marketplace-screenshots.githubusercontent.com/ copilotprodattachments.blob.core.windows.net/github-production-copilot-attachments/ github-production-user-asset-6210df.s3.amazonaws.com customer-stories-feed.github.com spotlights-feed.github.com objects-origin.githubusercontent.com *.githubusercontent.com images.ctfassets.net/8aevphvgewt8/; manifest-src 'self'; media-src github.com user-images.githubusercontent.com/ secured-user-images.githubusercontent.com/ private-user-images.githubusercontent.com github-production-user-asset-6210df.s3.amazonaws.com gist.github.com github.githubassets.com assets.ctfassets.net/8aevphvgewt8/ videos.ctfassets.net/8aevphvgewt8/; script-src github.githubassets.com; style-src 'unsafe-inline' github.githubassets.com; upgrade-insecure-requests; worker-src github.githubassets.com github.com/assets-cdn/worker/ github.com/assets/ gist.github.com/assets-cdn/worker/",
                  "Server": "github.com",
                  "Content-Encoding": "gzip",
                  "Accept-Ranges": "bytes",
                  "Set-Cookie": "_gh_sess=6TvxpKgpowlFcTHmKwUCZyQ6D%2BEpyP8awOc7VDV%2BSiiQ4KYSmmHNaUoycmGhqqaXDNtvbqTq%2B3zdLRFUOi1eATLks1oCejW5V%2BoFplzW6E1rs1LUJRsGjF6A3YhNYTIeYJKsidvA1b2mP1jC04z4qjlMTJgQGV%2FKbfMJx54swQdPaaLeTg9c0F3ZFbYxEKFSGs8IACdLgvRPL%2F5c5ctWna19HC%2Fjz7KI6itsTzHA3kZ60QHriqapvvIMHNgiGy5HCrH6AXQr75bBDBjHzRJn8Q%3D%3D--ht64Yc6QgEIDQN9R--4oPiIxQBP8FD4iqHSK1ycg%3D%3D; Path=/; HttpOnly; Secure; SameSite=Lax",
                  "X-GitHub-Request-Id": "A610:2D4EB:163229B:12C51F2:68CE0770",
                  "Connection": "close",
                  "Transfer-Encoding": "chunked"
                },
                "is_https": true,
                "technology_stack": [
                  "react",
                  "bootstrap",
                  "angular"
                ],
                "security_headers": {
                  "strict-transport-security": "max-age=31536000; includeSubdomains; preload",
                  "x-content-type-options": "nosniff",
                  "x-frame-options": "deny",
                  "x-xss-protection": "0",
            
                  "content-security-policy": "default-src 'none'; base-uri 'self'; child-src github.githubassets.com github.com/assets-cdn/worker/ github.com/assets/ gist.github.com/assets-cdn/worker/; connect-src 'self' uploads.github.com www.githubstatus.com collector.github.com raw.githubusercontent.com api.github.com github-cloud.s3.amazonaws.com github-production-repository-file-5c1aeb.s3.amazonaws.com github-production-upload-manifest-file-7fdce7.s3.amazonaws.com github-production-user-asset-6210df.s3.amazonaws.com *.rel.tunnels.api.visualstudio.com wss://*.rel.tunnels.api.visualstudio.com github.githubassets.com objects-origin.githubusercontent.com copilot-proxy.githubusercontent.com proxy.individual.githubcopilot.com proxy.business.githubcopilot.com proxy.enterprise.githubcopilot.com *.actions.githubusercontent.com wss://*.actions.githubusercontent.com productionresultssa0.blob.core.windows.net/ productionresultssa1.blob.core.windows.net/ productionresultssa2.blob.core.windows.net/ productionresultssa3.blob.core.windows.net/ productionresultssa4.blob.core.windows.net/ productionresultssa5.blob.core.windows.net/ productionresultssa6.blob.core.windows.net/ productionresultssa7.blob.core.windows.net/ productionresultssa8.blob.core.windows.net/ productionresultssa9.blob.core.windows.net/ productionresultssa10.blob.core.windows.net/ productionresultssa11.blob.core.windows.net/ productionresultssa12.blob.core.windows.net/ productionresultssa13.blob.core.windows.net/ productionresultssa14.blob.core.windows.net/ productionresultssa15.blob.core.windows.net/ productionresultssa16.blob.core.windows.net/ productionresultssa17.blob.core.windows.net/ productionresultssa18.blob.core.windows.net/ productionresultssa19.blob.core.windows.net/ github-production-repository-image-32fea6.s3.amazonaws.com github-production-release-asset-2e65be.s3.amazonaws.com insights.github.com wss://alive.github.com wss://alive-staging.github.com api.githubcopilot.com api.individual.githubcopilot.com api.business.githubcopilot.com api.enterprise.githubcopilot.com edge.fullstory.com rs.fullstory.com; font-src github.githubassets.com; form-action 'self' github.com gist.github.com copilot-workspace.githubnext.com objects-origin.githubusercontent.com; frame-ancestors 'none'; frame-src viewscreen.githubusercontent.com notebooks.githubusercontent.com www.youtube-nocookie.com; img-src 'self' data: blob: github.githubassets.com media.githubusercontent.com camo.githubusercontent.com identicons.github.com avatars.githubusercontent.com private-avatars.githubusercontent.com github-cloud.s3.amazonaws.com objects.githubusercontent.com release-assets.githubusercontent.com secured-user-images.githubusercontent.com/ user-images.githubusercontent.com/ private-user-images.githubusercontent.com opengraph.githubassets.com marketplace-screenshots.githubusercontent.com/ copilotprodattachments.blob.core.windows.net/github-production-copilot-attachments/ github-production-user-asset-6210df.s3.amazonaws.com customer-stories-feed.github.com spotlights-feed.github.com objects-origin.githubusercontent.com *.githubusercontent.com images.ctfassets.net/8aevphvgewt8/; manifest-src 'self'; media-src github.com user-images.githubusercontent.com/ secured-user-images.githubusercontent.com/ private-user-images.githubusercontent.com github-production-user-asset-6210df.s3.amazonaws.com gist.github.com github.githubassets.com assets.ctfassets.net/8aevphvgewt8/ videos.ctfassets.net/8aevphvgewt8/; script-src github.githubassets.com; style-src 'unsafe-inline' github.githubassets.com; upgrade-insecure-requests; worker-src github.githubassets.com github.com/assets-cdn/worker/ github.com/assets/ gist.github.com/assets-cdn/worker/",
                  "referrer-policy": "origin-when-cross-origin, strict-origin-when-cross-origin",
                  "permissions-policy": null,
                  "access-control-allow-origin": null,
                  "access-control-allow-methods": null
                },
                "content_analysis": {
                  "title": "GitHub · Build and ship software on a single, collaborative platform · GitHub",
                  "meta_tags": {
            
                    "route-pattern": "/",
                    "route-controller": "dashboard",
                    "route-action": "index",
                    "fetch-nonce": "v2:412f9d9d-f70f-17e4-e0b7-760cc0a2b8be",
                    "current-catalog-service-hash": "40dc28bd654b20f337468a532ff456ed5863889cfbb4e982b793597321d48d3f",
                    "request-id": "A610:2D4EB:163229B:12C51F2:68CE0770",
                    "html-safe-nonce": "69879e4eab57f88c284765665c93125698ad0f91755fb57cac52e0ba202fddfa",
                    "visitor-payload": "eyJyZWZlcnJlciI6IiIsInJlcXVlc3RfaWQiOiJBNjEwOjJENEVCOjE2MzIyOUI6MTJDNTFGMjo2OENFMDc3MCIsInZpc2l0b3JfaWQiOiI3NzAzNDcyNDk1MTIzMTA1NjQ4IiwicmVnaW9uX2VkZ2UiOiJmcmEiLCJyZWdpb25fcmVuZGVyIjoiZnJhIn0=",
                    "visitor-hmac": "7047b874026767d23d31ceacc73ac0326a67bee2ce0bbfa30ab19a995272ccd1",
                    "page-subject": "GitHub",
                    "github-keyboard-shortcuts": "dashboards,copilot",
                    "selected-link": null,
                    "google-site-verification": "Apib7-x98H0j5cPqHWwSMm6dNU4GmODRoqxLiDzdx9I",
                    "octolytics-url": "https://collector.github.com/github/collect",
                    "user-login": "",
                    "viewport": "width=device-width",
                    "description": "Join the world's most widely adopted, AI-powered developer platform where millions of developers, businesses, and the largest open source community build software that advances humanity.",
                    "fb:app_id": "1401488693436528",
                    "apple-itunes-app": "app-id=1477376905, app-argument=https://github.com/",
                    "twitter:image": "https://images.ctfassets.net/8aevphvgewt8/4UxhHBs2XnuyZ4lYQ83juV/b61529b087aeb4a318bda311edf4c345/home24.jpg",
                    "twitter:site": "@github",
                    "twitter:card": "summary_large_image",
                    "twitter:title": "GitHub · Build and ship software on a single, collaborative platform",
                    "twitter:description": "Join the world's most widely adopted, AI-powered developer platform where millions of developers, businesses, and the largest open source community build software that advances humanity.",
                    "og:image": "https://images.ctfassets.net/8aevphvgewt8/4UxhHBs2XnuyZ4lYQ83juV/b61529b087aeb4a318bda311edf4c345/home24.jpg",
                    "og:image:alt": "Join the world's most widely adopted, AI-powered developer platform where millions of developers, businesses, and the largest open source community build software that advances humanity.",
                    "og:site_name": "GitHub",
                    "og:type": "object",
                    "og:title": "GitHub · Build and ship software on a single, collaborative platform",
                    "og:url": "https://github.com/",
                    "og:description": "Join the world's most widely adopted, AI-powered developer platform where millions of developers, businesses, and the largest open source community build software that advances humanity.",
                    "hostname": "github.com",
                    "expected-hostname": "github.com",
                    "x-pjax-version": "bef24885bbd67361b388e8e0d0eb6a83e49ff8208b583d493099c958e166f2c1",
                    "x-pjax-csp-version": "13854b94e654841b18d7bbceea8296448cc8c5a1f7884c811da88056a0035ad3",
                    "x-pjax-css-version": "0bc51a290919c52cc62b3d8b4eed96609edf264f742d0409c975553b0cdc84a8",
                    "x-pjax-js-version": "5fb0b72757a8af0550d564211ab2885da1b8e95014f6dc26acc4c863e1a2268f",
                    "turbo-cache-control": "no-cache",
                    "is_logged_out_page": "true",
                    "octolytics-page-type": "marketing",
                    "octolytics-revenue-play": "Platform",
                    "turbo-body-classes": "logged-out env-production page-responsive header-overlay header-overlay-fixed js-header-overlay-fixed",
                    "browser-stats-url": "https://api.github.com/_private/browser/stats",
                    "browser-errors-url": "https://api.github.com/_private/browser/errors",
                    "release": "3a83c532047a42870ed81147b0dab76c3feb8943",
                    "ui-target": "full",
                    "theme-color": "#1e2327",
                    "color-scheme": "light dark"
                  },
                  "element_count": {
                    "links": 143,
                    "images": 29,
                    "scripts": 63,
                    "stylesheets": 29,
                    "forms": 5
                  }
                },
                "additional_checks": {
                  "robots_txt": {
                    "exists": true,
                    "status_code": 200,
            
                    "content_type": "text/plain"
                  },
                  "sitemap_xml": {
                    "exists": false,
                    "status_code": 406,
                    "content_type": "application/xml"
                  },
                  "favicon": {
                    "exists": true,
                    "content_type": "image/x-icon",
                    "size": "1219"
                  }
                },
                "redirect_chain": [],
                "ssl_info": {}
              },
              {
                "url": "http://neverssl.com",
                "timestamp": "2025-09-20T01:46:19.342912+00:00",
                "success": true,
                "error": null,
                "response_time": 0.6758143901824951,
                "status_code": 200,
                "content_type": "text/html; charset=UTF-8",
                "content_length": "1900",
                "headers": {
                  "Date": "Sat, 20 Sep 2025 01:46:24 GMT",
                  "Server": "Apache/2.4.62 ()",
                  "Upgrade": "h2,h2c",
                  "Connection": "Upgrade, close",
                  "Last-Modified": "Wed, 29 Jun 2022 00:23:33 GMT",
                  "Etag": "\\"f79-5e28b29d38e93-gzip\\"",
                  "Accept-Ranges": "bytes",
                  "Vary": "Accept-Encoding",
                  "Content-Encoding": "gzip",
                  "Content-Length": "1900",
                  "Content-Type": "text/html; charset=UTF-8"
                },
                "is_https": false,
                "technology_stack": [
                  "apache",
                  "angular"
                ],
                "security_headers": {
                  "strict-transport-security": null,
                  "x-content-type-options": null,
                  "x-frame-options": null,
                  "x-xss-protection": null,
                  "content-security-policy": null,
                  "referrer-policy": null,
                  "permissions-policy": null,
                  "access-control-allow-origin": null,
                  "access-control-allow-methods": null
                },
                "content_analysis": {
                  "title": "NeverSSL - Connecting ... ",
                  "meta_tags": {},
                  "element_count": {
                    "links": 2,
                    "images": 0,
                    "scripts": 2,
                    "stylesheets": 0,
                    "forms": 0
                  }
                },
                "additional_checks": {
                  "robots_txt": {
                    "exists": false
                  },
                  "sitemap_xml": {
                    "exists": false
                  },
                  "favicon": {
                    "exists": false
                  }
                },
                "redirect_chain": []
              }
            ]
            """;

    @BeforeEach
    void setUp() {
        converter = new KafkaMessageConverter();
    }

    @Test
    void testFullConvertChecks() {
        List<Check> checks = converter.toChecks(JSON_FULL);

        System.out.println(checks.toArray().length);
        checks.forEach(System.out::println);
        assertEquals(2, checks.size());
    }
}

