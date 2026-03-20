const ALLOWED_TAGS = new Set([
  "p", "br", "b", "i", "em", "strong", "ul", "ol", "li",
  "h1", "h2", "h3", "h4", "h5", "h6", "a", "span", "div",
  "table", "thead", "tbody", "tr", "th", "td", "blockquote",
  "code", "pre", "hr",
]);

const ALLOWED_ATTRS = new Set(["href", "target", "rel", "class"]);

export function sanitizeHtml(html: string): string {
  if (!html) return "";

  // Remove script tags and their content
  let clean = html.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, "");

  // Remove event handlers (onclick, onerror, etc.)
  clean = clean.replace(/\s+on\w+\s*=\s*["'][^"']*["']/gi, "");
  clean = clean.replace(/\s+on\w+\s*=\s*[^\s>]*/gi, "");

  // Remove javascript: urls
  clean = clean.replace(/href\s*=\s*["']?\s*javascript:/gi, 'href="');

  // Remove style tags
  clean = clean.replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, "");

  // Remove iframe, object, embed, form tags
  clean = clean.replace(/<\/?(iframe|object|embed|form|input|button|textarea|select)\b[^>]*>/gi, "");

  // Strip disallowed attributes from remaining tags
  clean = clean.replace(/<(\w+)((?:\s+[^>]*?)?)>/g, (match, tag, attrs) => {
    const tagLower = tag.toLowerCase();
    if (!ALLOWED_TAGS.has(tagLower)) {
      return "";
    }
    if (!attrs || !attrs.trim()) return `<${tag}>`;

    const cleanAttrs = attrs.replace(/\s+(\w[\w-]*)(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]*))?/g,
      (attrMatch: string, attrName: string) => {
        return ALLOWED_ATTRS.has(attrName.toLowerCase()) ? attrMatch : "";
      }
    );
    return `<${tag}${cleanAttrs}>`;
  });

  // Clean closing tags for disallowed elements
  clean = clean.replace(/<\/(\w+)>/g, (match, tag) => {
    return ALLOWED_TAGS.has(tag.toLowerCase()) ? match : "";
  });

  return clean;
}
