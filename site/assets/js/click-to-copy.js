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
      videoCard.innerHTML = `<iframe src="${embedUrl}" title="Agent Substrate Demo: ~250 stateful actors on 8 physical pods" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>`;
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

  // 1. Copy Page as Markdown buttons (header metadata row & right-rail article action area)
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

  // 2. Upgrade "NOTE: Much of this architecture is aspirational..." into the Design in progress callout
  const firstP = docBody.querySelector(':scope > p:first-child');
  if (firstP && /^NOTE:\s*Much of this architecture is aspirational/i.test(firstP.textContent.trim())) {
    const callout = document.createElement('div');
    callout.className = 'as-callout as-callout--warn';
    callout.setAttribute('role', 'note');
    callout.innerHTML = `
      <svg class="ic as-callout__icon" width="20" height="20" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l9.5 17h-19z"></path><path d="M12 10v4M12 17.5v.01"></path></svg>
      <div class="as-callout__body">
        <span class="as-callout__title">Design in progress</span>
        <span class="as-callout__text">Much of this architecture is aspirational and not yet implemented. Sections tagged <b>Planned</b> describe intended behavior. See the <a href="../../project/roadmap/">roadmap</a> for what ships next.</span>
      </div>
    `;
    firstP.replaceWith(callout);
  }

  // 3. Transform [Planned] in headings into pink pill badges and clean TOC links
  docBody.querySelectorAll('h2, h3, h4').forEach((heading) => {
    if (heading.innerHTML.includes('[Planned]')) {
      heading.innerHTML = heading.innerHTML.replace(
        /\s*\[Planned\]/g,
        ' <span class="as-badge-planned">Planned</span>'
      );
      heading.classList.add('as-heading-with-badge');
    } else if (heading.id === 'problems-we-havent-addressed-yet') {
      heading.insertAdjacentHTML('beforeend', ' <span class="as-badge-planned">Planned</span>');
      heading.classList.add('as-heading-with-badge');
    }
  });
  document.querySelectorAll('#TableOfContents a').forEach((link) => {
    if (link.textContent.includes('[Planned]')) {
      link.textContent = link.textContent.replace(/\s*\[Planned\]/g, '');
    }
  });

  // 4. Architecture page visual enhancements (4-step Actor Lifecycle cards, Figure 1 Resource Model diagram, and System Components table)
  const lifecycleHeading = docBody.querySelector('#actor-lifecycle');
  if (lifecycleHeading) {
    const nextEl = lifecycleHeading.nextElementSibling;
    const grid = document.createElement('div');
    grid.className = 'as-lifecycle-grid';
    grid.innerHTML = `
      <div class="as-lifecycle-card">
        <span class="mono as-lifecycle-card__num">01</span>
        <span class="as-lifecycle-card__title">Create</span>
        <span class="mono as-lifecycle-card__sub">CreateActor</span>
      </div>
      <div class="as-lifecycle-card as-lifecycle-card--hot">
        <span class="mono as-lifecycle-card__num">02 · hot path</span>
        <span class="as-lifecycle-card__title">Activate</span>
        <span class="mono as-lifecycle-card__sub">ResumeActor</span>
      </div>
      <div class="as-lifecycle-card">
        <span class="mono as-lifecycle-card__num">03</span>
        <span class="as-lifecycle-card__title">Hibernate</span>
        <span class="mono as-lifecycle-card__sub">SuspendActor</span>
      </div>
      <div class="as-lifecycle-card">
        <span class="mono as-lifecycle-card__num">04</span>
        <span class="as-lifecycle-card__title">Recover / delete</span>
        <span class="mono as-lifecycle-card__sub">RevertActor · DeleteActor</span>
      </div>
    `;
    const anchorEl = nextEl && nextEl.tagName === 'P' ? nextEl : lifecycleHeading;
    anchorEl.insertAdjacentElement('afterend', grid);

    // Collect UML sequence & state machine elements right after the grid until the first H3
    const umlEls = [];
    let scan = grid.nextElementSibling;
    while (scan && !/^H[1-6]$/.test(scan.tagName)) {
      umlEls.push(scan);
      scan = scan.nextElementSibling;
    }
    if (umlEls.some((el) => el.classList.contains('mermaid'))) {
      umlEls.forEach((el) => {
        el.hidden = true;
      });
      const toggleBar = document.createElement('div');
      toggleBar.className = 'as-res-figure__caption as-lifecycle-uml-bar';
      toggleBar.innerHTML = `
        <span>Four-phase actor lifecycle · only Activation sits on the hot request path.</span>
        <button type="button" class="as-res-figure__toggle" data-toggle-lifecycle-uml="true">View sequence &amp; state UML</button>
      `;
      grid.insertAdjacentElement('afterend', toggleBar);
      const btn = toggleBar.querySelector('[data-toggle-lifecycle-uml="true"]');
      if (btn) {
        btn.addEventListener('click', () => {
          const isHidden = umlEls[0].hidden;
          umlEls.forEach((el) => {
            el.hidden = !isHidden;
          });
          btn.textContent = isHidden ? 'Hide sequence & state UML' : 'View sequence & state UML';
        });
      }
    }
  }

  const sysCompHeading = docBody.querySelector('#system-components');
  if (sysCompHeading && !docBody.querySelector('.as-sys-comp-table')) {
    const tbl = document.createElement('table');
    tbl.className = 'as-sys-comp-table';
    tbl.innerHTML = `
      <thead>
        <tr>
          <th>Component</th>
          <th>Runs As</th>
          <th>Role</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td class="mono" style="color: #F4F8F5">ate-api-server</td>
          <td style="color: #9AA89F">Deployment</td>
          <td>Control plane gRPC API for actor and worker lifecycles</td>
        </tr>
        <tr>
          <td class="mono" style="color: #F4F8F5">atelet</td>
          <td style="color: #9AA89F">DaemonSet</td>
          <td>Supervises worker pods, coordinates snapshots and state transfer</td>
        </tr>
        <tr>
          <td class="mono" style="color: #F4F8F5">ateom</td>
          <td style="color: #9AA89F">In worker pod</td>
          <td>Runs checkpoint and restore for gVisor or micro-VM sandboxes</td>
        </tr>
        <tr>
          <td class="mono" style="color: #F4F8F5">atenet</td>
          <td style="color: #9AA89F">Envoy + sidecar</td>
          <td>Routes traffic and triggers on-demand resume</td>
        </tr>
        <tr>
          <td class="mono" style="color: #F4F8F5">atecontroller</td>
          <td style="color: #9AA89F">Controller</td>
          <td>Reconciles <code>WorkerPool</code> resources into Deployments</td>
        </tr>
      </tbody>
    `;
    sysCompHeading.insertAdjacentElement('afterend', tbl);
  }

  const resourceHeading = docBody.querySelector('#resource-model');
  if (resourceHeading) {
    let cursor = resourceHeading.nextElementSibling;
    while (cursor && !cursor.classList.contains('mermaid') && !/^H[1-6]$/.test(cursor.tagName)) {
      cursor = cursor.nextElementSibling;
    }
    const mermaidPre = cursor && cursor.classList.contains('mermaid') ? cursor : null;
    if (mermaidPre) {
      mermaidPre.classList.add('as-uml-collapsible');
      mermaidPre.hidden = true;

      const fig = document.createElement('figure');
      fig.className = 'as-res-figure';
      fig.innerHTML = `
        <div class="as-res-figure__bar">
          <span class="mono as-res-figure__bar-left">kube-apiserver</span>
          <span class="as-res-figure__bar-right">declarative · Kubernetes objects</span>
        </div>
        <div class="as-res-figure__row">
          <div class="as-res-node">
            <span class="as-res-node__tag as-res-node__tag--pink">CRD</span>
            <span class="mono as-res-node__name">WorkerPool</span>
          </div>
          <div class="as-res-edge">
            <span class="as-res-edge__label">atecontroller</span>
            <div class="as-res-edge__line as-res-edge__line--dashed">
              <svg class="ic" width="10" height="10" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 4l12 8-12 8"></path></svg>
            </div>
          </div>
          <div class="as-res-node">
            <span class="as-res-node__tag">K8s</span>
            <span class="mono as-res-node__name">Deployment</span>
          </div>
          <div class="as-res-edge">
            <span class="mono as-res-edge__label">1 &rarr; *</span>
            <div class="as-res-edge__line">
              <svg class="ic" width="10" height="10" viewBox="0 0 24 24" aria-hidden="true"><path d="M6 4l12 8-12 8"></path></svg>
            </div>
          </div>
          <div class="as-res-node as-res-node--stacked">
            <span class="as-res-node__tag">Pod</span>
            <span class="mono as-res-node__name">WorkerPod</span>
          </div>
        </div>
        <div class="as-res-figure__vlinks">
          <div class="as-res-vlink">
            <div class="as-res-vlink__line"></div>
            <span class="mono as-res-vlink__label">worker_selector</span>
          </div>
          <div></div><div></div><div></div>
          <div class="as-res-vlink">
            <div class="as-res-vlink__line"></div>
            <span class="mono as-res-vlink__label">podIP</span>
          </div>
        </div>
        <div class="as-res-figure__row">
          <div class="as-res-node as-res-node--record">
            <span class="as-res-node__tag as-res-node__tag--blue">record</span>
            <span class="mono as-res-node__name">ActorTemplate</span>
          </div>
          <div class="as-res-edge">
            <span class="mono as-res-edge__label">* &rarr; 1</span>
            <div class="as-res-edge__line as-res-edge__line--left">
              <svg class="ic" width="10" height="10" viewBox="0 0 24 24" aria-hidden="true"><path d="M18 4L6 12l12 8"></path></svg>
            </div>
          </div>
          <div class="as-res-node as-res-node--actor">
            <span class="as-res-node__tag as-res-node__tag--blue">record</span>
            <span class="mono as-res-node__name">Actor</span>
            <span class="mono as-res-node__sub">status · snapshotRefs</span>
          </div>
          <div class="as-res-edge">
            <span class="mono as-res-edge__label">actorName</span>
            <div class="as-res-edge__line as-res-edge__line--left">
              <svg class="ic" width="10" height="10" viewBox="0 0 24 24" aria-hidden="true"><path d="M18 4L6 12l12 8"></path></svg>
            </div>
          </div>
          <div class="as-res-node as-res-node--record">
            <span class="as-res-node__tag as-res-node__tag--blue">record</span>
            <span class="mono as-res-node__name">Worker</span>
          </div>
        </div>
        <div class="as-res-figure__bar">
          <span class="mono as-res-figure__bar-left">ate-api-server</span>
          <span class="as-res-figure__bar-right">dynamic · control-plane store</span>
        </div>
        <figcaption class="as-res-figure__caption">
          <span>Figure 1 · Resources and records, simplified.</span>
          <button type="button" class="as-res-figure__toggle" data-toggle-uml="true">View full UML</button>
        </figcaption>
      `;
      mermaidPre.insertAdjacentElement('beforebegin', fig);

      const toggleBtn = fig.querySelector('[data-toggle-uml="true"]');
      if (toggleBtn) {
        toggleBtn.addEventListener('click', () => {
          const isHidden = mermaidPre.hidden;
          mermaidPre.hidden = !isHidden;
          toggleBtn.textContent = isHidden ? 'Hide full UML' : 'View full UML';
        });
      }
    }
  }

  // 5. Programmatic Glossary Linking & Hover-Card Definitions for first occurrence of jargon terms
  if (window.location.pathname.includes('/reference/glossary')) {
    return;
  }

  const glossaryNavLink = document.querySelector('.as-sb a[href*="/reference/glossary/"]');
  const glossaryBase = glossaryNavLink
    ? glossaryNavLink.getAttribute('href').replace(/#.*$/, '')
    : '/substrate/docs/reference/glossary/';

  const GLOSSARY_TERMS = [
    {
      key: 'atespace',
      anchor: 'atespace',
      pattern: /\b(atespaces?)\b/i,
      title: 'Atespace',
      def: 'The global isolation boundary an Actor and ActorTemplate belong to, and the first half of an actor\u2019s <code>(atespace, name)</code> address.',
    },
    {
      key: 'atelet',
      anchor: 'atelet',
      pattern: /\b(atelet)\b/i,
      title: 'atelet',
      def: 'The node-level supervisor DaemonSet. Pulls images, drives sandboxes on the node via <code>ateom</code>, and streams snapshots to/from object storage.',
    },
    {
      key: 'ateom',
      anchor: 'ateom',
      pattern: /\b(ateom)\b/i,
      title: 'ateom',
      def: 'The coordinator inside each worker Pod that drives the gVisor or microVM sandbox runtime on behalf of <code>atelet</code> and hosts <code>atunnel</code>.',
    },
    {
      key: 'atenet',
      anchor: 'atenet',
      pattern: /\b(atenet(?:-router)?)\b/i,
      title: 'atenet',
      def: 'The Agent Substrate networking stack and Envoy-based router that intercepts traffic, triggers on-demand Actor resume, and tunnels to the assigned worker.',
    },
    {
      key: 'ate',
      anchor: 'ate',
      pattern: /\b(ATE|kubectl-ate|ate-api-server|ate-system)\b/,
      title: 'ATE vs. Agent Substrate',
      def: '<strong>Agent Substrate</strong> is the platform runtime; <code>ate</code> (Agent Task/Execution Engine) is the internal prefix used across binaries (<code>atelet</code>, <code>ateom</code>, <code>atenet</code>), CRDs, and <code>kubectl-ate</code>.',
    },
    {
      key: 'actor',
      anchor: 'actor',
      pattern: /\b(actors?)\b/i,
      title: 'Actor',
      def: 'An instance of an agent-like workload, addressed by <code>(atespace, name)</code>. Suspended when idle, resumed on any ready worker.',
    },
    {
      key: 'worker',
      anchor: 'worker',
      pattern: /\b(workers?)\b/i,
      title: 'Worker',
      def: 'A pre-warmed Kubernetes Pod running a gVisor or microVM sandbox that hosts an active actor on demand.',
    },
  ];

  const SKIP_SELECTOR =
    'pre, a, button, script, style, h1, h2, h3, h4, h5, h6, th, figcaption, .as-callout, .as-glossary-wrap, .as-diagram-card, .as-res-figure, .as-lifecycle-grid, .code-tabs__nav';

  function buildGlossaryNode(term, contentNode) {
    const href = `${glossaryBase}#${term.anchor}`;
    const wrap = document.createElement('span');
    wrap.className = 'as-glossary-wrap';
    wrap.setAttribute('data-glossary', term.key);

    const link = document.createElement('a');
    link.className = 'as-glossary-word';
    link.href = href;
    link.appendChild(contentNode);

    const pop = document.createElement('span');
    pop.className = 'as-glossary-pop';
    pop.setAttribute('role', 'tooltip');
    pop.innerHTML = `
      <span class="as-glossary-pop__eyebrow">GLOSSARY</span>
      <span class="as-glossary-pop__title">${term.title}</span>
      <span class="as-glossary-pop__desc">${term.def}</span>
      <a href="${href}" class="as-glossary-pop__link">Open in glossary &rarr;</a>
    `;

    wrap.appendChild(link);
    wrap.appendChild(pop);
    return wrap;
  }

  GLOSSARY_TERMS.forEach((term) => {
    if (docBody.querySelector(`[data-glossary="${term.key}"]`)) return;

    const walker = document.createTreeWalker(docBody, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        if (!node.nodeValue || !node.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
        const parent = node.parentElement;
        if (!parent || parent.closest(SKIP_SELECTOR)) return NodeFilter.FILTER_REJECT;
        if (!parent.closest('p, li, td')) return NodeFilter.FILTER_REJECT;
        return term.pattern.test(node.nodeValue) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_SKIP;
      },
    });

    const matchNode = walker.nextNode();
    if (!matchNode) return;

    const parent = matchNode.parentElement;
    const text = matchNode.nodeValue;
    const match = term.pattern.exec(text);
    if (!match) return;

    // If the text node is the entire content of an inline <code> tag, wrap the <code> tag inside the glossary link
    if (parent && parent.tagName === 'CODE' && text.trim() === match[0]) {
      const codeClone = parent.cloneNode(true);
      const glossaryEl = buildGlossaryNode(term, codeClone);
      parent.replaceWith(glossaryEl);
      return;
    }

    const startIdx = match.index;
    const matchedStr = match[0];
    const before = text.slice(0, startIdx);
    const after = text.slice(startIdx + matchedStr.length);

    const frag = document.createDocumentFragment();
    if (before) frag.appendChild(document.createTextNode(before));
    frag.appendChild(buildGlossaryNode(term, document.createTextNode(matchedStr)));
    if (after) frag.appendChild(document.createTextNode(after));

    matchNode.replaceWith(frag);
  });
}

initCodeTabs();
initCodeBlocks();
initHomeRedesign();
initDocsInnerRedesign();
