// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Glossary hover cards: marks the first mention of each glossary term on a
// doc page with a card showing its definition. Terms and definitions come
// from glossary.json, which the site builds from docs/glossary.md; nothing
// here is specific to a term or a page.
(function () {
  // Characters that continue a term, so "ate" does not match in
  // "ate-api-server" and "Actor" does not match in "ACTOR_STATE".
  const WORD = 'A-Za-z0-9_-';

  function escapeRegExp(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  // Matches the term with an optional plural "s", bounded by non-word characters.
  function termPattern(term, ignoreCase) {
    return new RegExp(`(?<![${WORD}])${escapeRegExp(term)}s?(?![${WORD}])`, ignoreCase ? 'i' : '');
  }

  // Longer terms first, so "ActorTemplate" is claimed before "Actor".
  function sortTerms(terms) {
    return [...terms].sort((a, b) => b.term.length - a.term.length);
  }

  function findMatch(text, pattern) {
    const m = pattern.exec(text);
    return m ? { index: m.index, length: m[0].length } : null;
  }

  // Picks the mention to decorate: the first use in the glossary's casing, or
  // failing that the first use in any casing. A text marked in wholeOnly (an
  // inline code span) counts only when the term is all of it, so a card never
  // lands inside an identifier such as ate.workerpool.workers.
  function pickMention(texts, term, wholeOnly = []) {
    for (const ignoreCase of [false, true]) {
      const pattern = termPattern(term, ignoreCase);
      for (let i = 0; i < texts.length; i++) {
        const m = findMatch(texts[i], pattern);
        if (!m) continue;
        if (wholeOnly[i] && texts[i].slice(m.index, m.index + m.length) !== texts[i].trim()) continue;
        return { textIndex: i, index: m.index, length: m.length };
      }
    }
    return null;
  }

  // Text inside these is never decorated. Tables clip their cells, which
  // would cut a card off at the table border.
  const SKIP = 'pre, a, h1, h2, h3, h4, h5, h6, button, table, .as-status-banner, .as-glossary-wrap';

  function eligibleTextNodes(root) {
    const nodes = [];
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        const parent = node.parentElement;
        if (!parent || !node.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
        if (parent.closest(SKIP) || !parent.closest('p, li')) return NodeFilter.FILTER_REJECT;
        return NodeFilter.FILTER_ACCEPT;
      },
    });
    for (let n = walker.nextNode(); n; n = walker.nextNode()) nodes.push(n);
    return nodes;
  }

  function span(className, text) {
    const el = document.createElement('span');
    el.className = className;
    if (text !== undefined) el.textContent = text;
    return el;
  }

  function buildCard(entry, glossaryURL, content) {
    const href = `${glossaryURL}#${entry.id}`;
    const descId = `as-glossary-${entry.id}`;

    // The term itself links to the glossary, so screen readers hear just the
    // definition and the card's own links stay out of the Tab order.
    const word = document.createElement('a');
    word.className = 'as-glossary-word';
    word.href = href;
    word.setAttribute('aria-describedby', descId);
    word.appendChild(content);

    const pop = span('as-glossary-pop');
    pop.setAttribute('role', 'tooltip');
    const desc = span('as-glossary-pop__desc');
    desc.id = descId;
    // Hugo's rendering of docs/glossary.md: trusted repository content.
    desc.innerHTML = entry.html;
    const link = document.createElement('a');
    link.className = 'as-glossary-pop__link';
    link.href = href;
    link.textContent = 'Open in glossary →';
    desc.querySelectorAll('a').forEach((a) => a.setAttribute('tabindex', '-1'));
    link.setAttribute('tabindex', '-1');
    pop.append(span('as-glossary-pop__eyebrow', 'GLOSSARY'), span('as-glossary-pop__title', entry.term), desc, link);

    const wrap = span('as-glossary-wrap');
    wrap.append(word, pop);
    return wrap;
  }

  function decorate(node, match, entry, glossaryURL) {
    const text = node.nodeValue;
    const hit = text.slice(match.index, match.index + match.length);
    const parent = node.parentElement;
    // A term that is a whole inline code span keeps its code styling, and the
    // modifier class lets the chip's own border mark the link.
    if (parent.tagName === 'CODE' && hit === text.trim()) {
      const card = buildCard(entry, glossaryURL, parent.cloneNode(true));
      card.querySelector('.as-glossary-word').classList.add('as-glossary-word--code');
      parent.replaceWith(card);
      return;
    }
    const frag = document.createDocumentFragment();
    const before = text.slice(0, match.index);
    const after = text.slice(match.index + match.length);
    if (before) frag.append(before);
    frag.append(buildCard(entry, glossaryURL, document.createTextNode(hit)));
    if (after) frag.append(after);
    node.replaceWith(frag);
  }

  async function initGlossaryCards() {
    const meta = document.getElementById('as-glossary-meta');
    const root = document.querySelector('.as-doc-body');
    if (!meta || !root) return;
    let data;
    try {
      const res = await fetch(JSON.parse(meta.textContent).dataURL);
      if (!res.ok) return;
      data = await res.json();
    } catch (e) {
      return;
    }
    if (window.location.pathname === data.glossaryURL) return;

    for (const entry of sortTerms(data.terms)) {
      const nodes = eligibleTextNodes(root);
      const pick = pickMention(
        nodes.map((n) => n.nodeValue),
        entry.term,
        nodes.map((n) => n.parentElement.tagName === 'CODE'),
      );
      if (pick) decorate(nodes[pick.textIndex], pick, entry, data.glossaryURL);
    }

    document.addEventListener('keydown', (e) => {
      const el = document.activeElement;
      if (e.key === 'Escape' && el && el.closest('.as-glossary-wrap')) el.blur();
    });
  }

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { termPattern, sortTerms, findMatch, pickMention };
  } else {
    initGlossaryCards();
  }
})();
