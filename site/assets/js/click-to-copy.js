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

const LANG_LABELS = {
  yaml: 'YAML',
  yml: 'YAML',
  bash: 'Bash',
  sh: 'Shell',
  shell: 'Shell',
  zsh: 'Shell',
  json: 'JSON',
  go: 'Go',
  golang: 'Go',
  proto: 'Protobuf',
  protobuf: 'Protobuf',
  python: 'Python',
  py: 'Python',
  toml: 'TOML',
  sql: 'SQL',
  html: 'HTML',
  xml: 'XML',
  css: 'CSS',
  scss: 'SCSS',
  js: 'JS',
  javascript: 'JS',
  ts: 'TS',
  typescript: 'TS',
  dockerfile: 'Dockerfile',
  makefile: 'Makefile',
  text: 'Text',
  plaintext: 'Text',
  txt: 'Text',
  fallback: 'Text',
};

function formatLangLabel(codeEl) {
  if (!codeEl) return 'Text';
  let raw = (codeEl.getAttribute('data-lang') || '').toLowerCase().trim();
  if (!raw) {
    const match = (codeEl.className || '').match(/language-([a-z0-9_-]+)/i);
    if (match) raw = match[1].toLowerCase();
  }
  if (!raw) return 'Text';
  return LANG_LABELS[raw] || raw.toUpperCase();
}

function initCodeTabs() {
  const tabGroups = document.querySelectorAll('.td-content .code-tabs, .td-home .code-tabs');
  tabGroups.forEach((group, groupIdx) => {
    const items = Array.from(group.querySelectorAll(':scope > .code-tab'));
    if (!items.length) return;

    const nav = document.createElement('ul');
    nav.className = 'nav nav-tabs code-tabs__nav';
    nav.setAttribute('role', 'tablist');

    const content = document.createElement('div');
    content.className = 'tab-content code-tabs__content';

    items.forEach((item, itemIdx) => {
      const label = item.getAttribute('data-tab') || `Tab ${itemIdx + 1}`;
      const persistKey = (item.getAttribute('data-persist') || label).toLowerCase().trim();
      const tabId = `code-tab-${groupIdx}-${itemIdx}`;
      const paneId = `code-pane-${groupIdx}-${itemIdx}`;
      const isActive = itemIdx === 0;

      const li = document.createElement('li');
      li.className = 'nav-item';
      li.setAttribute('role', 'presentation');

      const btn = document.createElement('button');
      btn.className = `nav-link${isActive ? ' active' : ''}`;
      btn.id = tabId;
      btn.type = 'button';
      btn.setAttribute('role', 'tab');
      btn.setAttribute('data-bs-toggle', 'tab');
      btn.setAttribute('data-bs-target', `#${paneId}`);
      btn.setAttribute('data-td-tp-persist', persistKey);
      btn.setAttribute('aria-controls', paneId);
      btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
      btn.textContent = label;

      li.appendChild(btn);
      nav.appendChild(li);

      const pane = document.createElement('div');
      pane.className = `tab-body tab-pane fade${isActive ? ' show active' : ''}`;
      pane.id = paneId;
      pane.setAttribute('role', 'tabpanel');
      pane.setAttribute('aria-labelledby', tabId);
      pane.setAttribute('tabindex', '0');

      while (item.firstChild) {
        pane.appendChild(item.firstChild);
      }
      content.appendChild(pane);
    });

    group.replaceWith(nav, content);
  });
}

function initCodeBlocks() {
  const codeListings = document.querySelectorAll('.td-content .highlight > pre, .td-home .highlight > pre');

  for (let index = 0; index < codeListings.length; index++) {
    const pre = codeListings[index];
    const container = pre.parentElement || pre;
    if (container.querySelector('.code-block-toolbar')) continue;

    const codeSample = pre.querySelector('code');
    if (!codeSample) continue;

    const toolbar = document.createElement('div');
    toolbar.className = 'click-to-copy code-block-toolbar';

    const langBadge = document.createElement('span');
    langBadge.className = 'code-lang-badge';
    langBadge.textContent = formatLangLabel(codeSample);
    toolbar.appendChild(langBadge);

    const copyButton = document.createElement('button');
    copyButton.type = 'button';
    copyButton.title = 'Copy to clipboard';
    copyButton.setAttribute('aria-label', 'Copy to clipboard');
    copyButton.className = 'btn btn-sm td-click-to-copy';

    const icon = document.createElement('i');
    icon.className = 'fa-regular fa-copy';
    icon.setAttribute('aria-hidden', 'true');

    const label = document.createElement('span');
    label.className = 'td-copy-label';
    label.textContent = 'Copy';

    copyButton.appendChild(icon);
    copyButton.appendChild(label);

    let resetTimer = null;
    copyButton.addEventListener('click', () => {
      const steps = codeSample.querySelectorAll('.cli-step');
      const text = steps.length
        ? Array.from(steps).map((s) => s.textContent.trim()).join('\n\n') + '\n'
        : codeSample.textContent.replace(/\n$/, '') + '\n';
      navigator.clipboard.writeText(text);
      copyButton.classList.add('copied');
      icon.className = 'fa-solid fa-check';
      label.textContent = 'Copied!';
      if (resetTimer) clearTimeout(resetTimer);
      resetTimer = setTimeout(() => {
        copyButton.classList.remove('copied');
        icon.className = 'fa-regular fa-copy';
        label.textContent = 'Copy';
      }, 1800);
    });

    toolbar.appendChild(copyButton);
    container.appendChild(toolbar);
  }
}

function initHomeRedesign() {
  const CHECK_SVG = '<svg class="ic" width="15" height="15" viewBox="0 0 24 24" aria-hidden="true"><path d="M20 6L9 17l-5-5"></path></svg>';
  const COPY_SVG = '<svg class="ic" width="15" height="15" viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="12" height="12" rx="2"></rect><path d="M5 15V5a2 2 0 0 1 2-2h10"></path></svg>';

  document.querySelectorAll('[data-term-tab]').forEach((tabBtn) => {
    tabBtn.addEventListener('click', () => {
      const targetKey = tabBtn.getAttribute('data-term-tab');
      const card = tabBtn.closest('.as-term-card');
      if (!card) return;
      card.querySelectorAll('[data-term-tab]').forEach((btn) => {
        const active = btn.getAttribute('data-term-tab') === targetKey;
        btn.classList.toggle('is-active', active);
        btn.setAttribute('aria-selected', active ? 'true' : 'false');
      });
      card.querySelectorAll('[data-term-pane]').forEach((pane) => {
        const active = pane.getAttribute('data-term-pane') === targetKey;
        pane.classList.toggle('is-active', active);
        pane.hidden = !active;
      });
    });
  });

  document.querySelectorAll('[data-copy-target]').forEach((btn) => {
    let timer = null;
    btn.addEventListener('click', () => {
      const el = document.getElementById(btn.getAttribute('data-copy-target'));
      if (!el) return;
      navigator.clipboard.writeText(el.textContent.trim() + '\n');
      btn.classList.add('is-copied');
      btn.innerHTML = CHECK_SVG;
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => {
        btn.classList.remove('is-copied');
        btn.innerHTML = COPY_SVG;
      }, 1600);
    });
  });

  document.querySelectorAll('[data-copy-active-term]').forEach((btn) => {
    let timer = null;
    btn.addEventListener('click', () => {
      const card = btn.closest('.as-term-card');
      if (!card) return;
      const pane = card.querySelector('.as-term-card__pre.is-active');
      if (!pane) return;
      navigator.clipboard.writeText(pane.textContent.trim() + '\n');
      btn.classList.add('is-copied');
      btn.innerHTML = CHECK_SVG;
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => {
        btn.classList.remove('is-copied');
        btn.innerHTML = COPY_SVG;
      }, 1600);
    });
  });

  const videoCard = document.getElementById('home-demo-video-card');
  if (videoCard) {
    videoCard.addEventListener('click', (e) => {
      const embedUrl = videoCard.getAttribute('data-youtube-embed');
      if (!embedUrl || videoCard.classList.contains('is-playing')) return;
      e.preventDefault();
      videoCard.classList.add('is-playing');
      videoCard.removeAttribute('href');
      const iframe = document.createElement('iframe');
      iframe.src = embedUrl;
      iframe.title = videoCard.getAttribute('data-youtube-title') || 'YouTube video';
      iframe.allow = 'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share';
      iframe.referrerPolicy = 'strict-origin-when-cross-origin';
      iframe.allowFullscreen = true;
      videoCard.replaceChildren(iframe);
    });
  }

  document.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      const searchInput = document.querySelector('.as-navbar__search .td-search__input');
      if (searchInput) {
        e.preventDefault();
        searchInput.focus();
      }
    }
  });
}

function initDocsInnerRedesign() {
  const docBody = document.querySelector('.as-doc-body');
  if (!docBody) return;

  // Copy Page as Markdown buttons (header metadata row & right-rail article action area)
  const copyPageBtns = document.querySelectorAll('[data-copy-page="true"]');
  if (copyPageBtns.length) {
    copyPageBtns.forEach((copyPageBtn) => {
      let timer = null;
      const labelEl = copyPageBtn.querySelector('.as-copy-page-btn__label');
      const defaultLabel = labelEl ? labelEl.textContent : 'Copy as Markdown';
      copyPageBtn.addEventListener('click', () => {
        let md = '';
        const rawScript = document.getElementById('as-page-markdown');
        if (rawScript) {
          try {
            md = JSON.parse(rawScript.textContent);
          } catch (_) {
            md = '';
          }
        }
        if (!md) {
          const title = document.querySelector('.as-doc-header h1')?.textContent.trim() || '';
          const lead = document.querySelector('.as-doc-lead')?.textContent.trim() || '';
          const bodyText = docBody.innerText.trim();
          md = `# ${title}\n\n${lead ? '> ' + lead + '\n\n' : ''}${bodyText}\n`;
        }
        navigator.clipboard.writeText(md);
        copyPageBtn.classList.add('is-copied');
        if (labelEl) labelEl.textContent = 'Copied Markdown!';
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => {
          copyPageBtn.classList.remove('is-copied');
          if (labelEl) labelEl.textContent = defaultLabel;
        }, 1800);
      });
    });
  }
}

initCodeTabs();
initCodeBlocks();
initHomeRedesign();
initDocsInnerRedesign();
