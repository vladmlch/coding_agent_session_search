import {
  collectBrowserErrors,
  countMessages,
  expect,
  gotoFile,
  test,
  waitForPageReady,
} from "../setup/test-utils";

/**
 * Runs in the page (self-contained): the computed text style of a fenced
 * code block and the WCAG contrast of its text over its `pre` background.
 * Colors go through a canvas so oklch() and rgb() compare alike.
 */
function readCodeBlock(code: Element) {
  const pre = code.closest("pre") ?? code;
  const own = window.getComputedStyle(code);
  const block = window.getComputedStyle(pre);
  const ctx = document.createElement("canvas").getContext("2d") as CanvasRenderingContext2D;
  const luminance = (css: string) => {
    ctx.clearRect(0, 0, 1, 1);
    ctx.fillStyle = "#000";
    ctx.fillStyle = css;
    ctx.fillRect(0, 0, 1, 1);
    const [r, g, b] = Array.from(ctx.getImageData(0, 0, 1, 1).data).map((v) => {
      const c = v / 255;
      return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
    });
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };
  const fg = luminance(own.color);
  const bg = luminance(block.backgroundColor);
  return {
    text: code.textContent ?? "",
    whiteSpace: own.whiteSpace,
    fontFamily: own.fontFamily,
    color: own.color,
    background: block.backgroundColor,
    contrast: (Math.max(fg, bg) + 0.05) / (Math.min(fg, bg) + 0.05),
  };
}

test.describe("Self-Contained Export", () => {
  test("renders fully without any CDN", async ({ page, exportPath }) => {
    test.skip(!exportPath, "Export path not available");
    const browserErrors = collectBrowserErrors(page);

    await gotoFile(page, exportPath);
    await waitForPageReady(page);

    // Page should render completely
    const messageCount = await countMessages(page);
    expect(messageCount).toBeGreaterThan(0);

    // Should be styled (has some CSS applied)
    const bodyBgColor = await page
      .locator("body")
      .evaluate((el) => window.getComputedStyle(el).backgroundColor);
    expect(bodyBgColor).not.toBe("");
    await page.waitForTimeout(500);
    expect(browserErrors.pageErrors).toEqual([]);
    expect(browserErrors.consoleErrors).toEqual([]);
  });

  test("issues zero network requests", async ({ page, exportPath }) => {
    test.skip(!exportPath, "Export path not available");

    const requests: string[] = [];
    page.on("request", (request) => requests.push(request.url()));
    const failures: string[] = [];
    page.on("requestfailed", (request) => failures.push(request.url()));

    await gotoFile(page, exportPath);
    await waitForPageReady(page);
    await page.waitForTimeout(1000);

    // The export is always self-contained: the only request the browser may
    // issue is the file:// navigation itself. Any external fetch is a
    // regression — there is no CDN fallback path left to save it.
    const external = requests.filter((url) => !url.startsWith("file://") && !url.startsWith("data:") && !url.startsWith("blob:"));
    expect(external, `external requests: ${external.join(", ")}`).toEqual([]);
    expect(failures, `failed requests: ${failures.join(", ")}`).toEqual([]);
  });

  test("no external resource URLs in export", async ({ page, exportPath }) => {
    test.skip(!exportPath, "Export path not available");

    await gotoFile(page, exportPath);
    const html = await page.content();

    const cdnPatterns = [
      "cdn.tailwindcss.com",
      "cdn.jsdelivr.net",
      "fonts.googleapis.com",
      "unpkg.com",
      "cdnjs.cloudflare.com",
    ];

    for (const pattern of cdnPatterns) {
      expect(html.includes(pattern), `external pattern ${pattern} in export`).toBe(false);
    }

    // SRI attributes and preconnect hints only ever exist for remote assets.
    expect(html).not.toMatch(/<link[^>]+rel="preconnect"/);
    expect(html).not.toMatch(/<script[^>]+\bintegrity=/);

    // No active script/link/img tag may point at a remote URL.
    const remoteTags = await page.evaluate(() => {
      const bad: string[] = [];
      for (const el of document.querySelectorAll("script[src], link[href], img[src], iframe[src]")) {
        const url = el.getAttribute("src") ?? el.getAttribute("href") ?? "";
        if (url && !url.startsWith("file://") && !url.startsWith("data:") && !url.startsWith("#")) {
          bad.push(`${el.tagName.toLowerCase()}: ${url}`);
        }
      }
      return bad;
    });
    expect(remoteTags, `remote resource tags: ${remoteTags.join(", ")}`).toEqual([]);
  });

  test("code blocks stay legible in both themes", async ({ page, exportPath }, testInfo) => {
    test.skip(!exportPath, "Export path not available");

    await gotoFile(page, exportPath);
    await waitForPageReady(page);

    const fencedCode = page.locator('pre > code[class*="language-"]').first();
    if ((await fencedCode.count()) === 0) {
      testInfo.skip(true, "fixture has no language-tagged code block");
      return;
    }
    await fencedCode.scrollIntoViewIfNeeded();
    await expect(fencedCode).toBeVisible();

    const codeReadability: Record<string, unknown> = {};
    for (const theme of ["dark", "light"]) {
      await page.evaluate((t) => document.documentElement.setAttribute("data-theme", t), theme);
      const code = await fencedCode.evaluate(readCodeBlock);
      codeReadability[theme] = code;
      expect(code.whiteSpace, `${theme}: line breaks preserved`).toMatch(/^pre/);
      expect(code.fontFamily.toLowerCase(), `${theme}: monospace`).toMatch(
        /mono|courier|consolas|menlo/,
      );
      expect(
        code.contrast,
        `${theme}: code text ${code.color} on ${code.background}`,
      ).toBeGreaterThanOrEqual(4.5);
    }

    await testInfo.attach("code-legibility", {
      body: Buffer.from(JSON.stringify(codeReadability, null, 2)),
      contentType: "application/json",
    });
  });
});

test.describe("Offline Mode Simulation", () => {
  test("page works in offline mode", async ({ page, exportPath, browserName }) => {
    // WebKit skip must be FIRST - setOffline fails immediately on WebKit with file:// URLs
    test.skip(browserName === "webkit", "WebKit offline mode not reliable with file:// URLs");
    test.skip(!exportPath, "Export path not available");

    // Go offline
    await page.context().setOffline(true);

    await gotoFile(page, exportPath);
    await waitForPageReady(page);

    // Page should work fully offline
    const messageCount = await countMessages(page);
    expect(messageCount).toBeGreaterThan(0);

    // Go back online
    await page.context().setOffline(false);
  });

  test("all critical styles are inline", async ({ page, exportPath }) => {
    test.skip(!exportPath, "Export path not available");

    await gotoFile(page, exportPath);
    await waitForPageReady(page);

    // Check that there are inline styles
    const inlineStyles = page.locator("style");
    const styleCount = await inlineStyles.count();
    expect(styleCount).toBeGreaterThan(0);

    // Critical styles should be present
    const html = await page.content();
    expect(html).toMatch(/\.message|\.conversation|body\s*\{/);
  });
});
