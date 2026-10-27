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

  initHeroVizAnimation();
}

function initHeroVizAnimation() {
  if (window.__asHeroVizStarted) return;
  const dotsContainer = document.getElementById('as-viz-dots');
  const podsContainer = document.getElementById('as-viz-pods');
  if (!dotsContainer || !podsContainer) return;

  const dots = Array.from(dotsContainer.querySelectorAll('.as-viz__dot'));
  const podEls = Array.from(podsContainer.querySelectorAll('.as-viz__pod'));
  if (!dots.length || !podEls.length) return;
  window.__asHeroVizStarted = true;

  const runningCountEl = document.querySelector('[data-viz-running-count]');
  const resumingCountEl = document.querySelector('[data-viz-resuming-count]');
  const suspendingCountEl = document.querySelector('[data-viz-suspending-count]');
  const routerPill = document.getElementById('as-viz-router-pill');

  const COLORS = ['green', 'blue', 'yellow', 'red'];
  const COLOR_HEX = {
    green: '#61D76B',
    blue: '#64b9de',
    yellow: '#F3F17B',
    red: '#E77CB2',
  };
  const COLOR_GLOW = {
    green: 'rgba(97, 215, 107, 0.45)',
    blue: 'rgba(100, 185, 222, 0.45)',
    yellow: 'rgba(243, 241, 123, 0.45)',
    red: 'rgba(231, 124, 178, 0.45)',
  };
  const WORKLOAD_PREFIXES = [
    'counter',
    'claude',
    'sandbox',
    'adk',
    'mcp',
    'hermes',
    'codex',
    'agent',
    'browser',
    'rag',
  ];

  // Divisors of the 30-second master loop (5s, 6s, 7.5s, 10s)
  const CYCLE_DURATIONS_MS = [5000, 6000, 7500, 10000];
  const PHASE_SPLITS = {
    5000: { resumeMs: 1200, runMs: 2800, suspendMs: 1000 },
    6000: { resumeMs: 1350, runMs: 3500, suspendMs: 1150 },
    7500: { resumeMs: 1500, runMs: 4700, suspendMs: 1300 },
    10000: { resumeMs: 1700, runMs: 6800, suspendMs: 1500 },
  };
  const initialCyclesMs = [5000, 6000, 7500, 10000, 6000, 5000, 10000, 7500];
  const initialStaggerMs = [600, 1800, 1100, 3000, 900, 2400, 4200, 5400];

  const now = performance.now();

  const workers = podEls.map((el, idx) => {
    const dotIndex = parseInt(el.getAttribute('data-dot-index') || '0', 10);
    const color = el.getAttribute('data-actor-color') || COLORS[idx % COLORS.length];
    const state = el.getAttribute('data-actor-state') || 'running';
    const nameEl = el.querySelector('.as-viz__pod-name');
    const subEl = el.querySelector('.as-viz__pod-sub');
    const dotEl = el.querySelector('.as-viz__pod-dot');
    const barEl = el.querySelector('.as-viz__pod-bar');
    const rawName = (nameEl ? nameEl.textContent : '').replace(/^actor\//, '').trim() || `agent-${dotIndex}`;
    const widthMatch = (barEl?.getAttribute('style') || '').match(/width:\s*(\d+)%/);
    const width = widthMatch ? parseInt(widthMatch[1], 10) : 68;
    const cycleMs = initialCyclesMs[idx % initialCyclesMs.length];
    const split = PHASE_SPLITS[cycleMs];

    return {
      idx,
      el,
      nameEl,
      subEl,
      dotEl,
      barEl,
      dotIndex,
      actorName: rawName,
      color,
      state,
      width,
      cycleMs,
      resumeMs: split.resumeMs,
      runMs: split.runMs,
      suspendMs: split.suspendMs,
      nextTransitionAt: now + initialStaggerMs[idx % initialStaggerMs.length],
    };
  });

  function clearPairedHover() {
    dots.forEach((d) => d.classList.remove('is-paired-hover'));
    podEls.forEach((p) => p.classList.remove('is-paired-hover'));
  }

  podEls.forEach((podEl, idx) => {
    podEl.addEventListener('mouseenter', () => {
      clearPairedHover();
      podEl.classList.add('is-paired-hover');
      const d = dots[workers[idx].dotIndex];
      if (d) d.classList.add('is-paired-hover');
    });
    podEl.addEventListener('mouseleave', clearPairedHover);
  });

  dots.forEach((dotEl, dotIdx) => {
    dotEl.addEventListener('mouseenter', () => {
      const w = workers.find((item) => item.dotIndex === dotIdx);
      if (!w) return;
      clearPairedHover();
      dotEl.classList.add('is-paired-hover');
      w.el.classList.add('is-paired-hover');
    });
    dotEl.addEventListener('mouseleave', clearPairedHover);
  });

  let routerPulseTimer = null;
  function pulseRouter(color) {
    if (!routerPill) return;
    routerPill.style.setProperty('--router-accent', COLOR_HEX[color] || '#64b9de');
    routerPill.style.setProperty('--router-glow', COLOR_GLOW[color] || 'rgba(100, 185, 222, 0.45)');
    routerPill.classList.add('is-routing');
    if (routerPulseTimer) clearTimeout(routerPulseTimer);
    routerPulseTimer = setTimeout(() => {
      routerPill.classList.remove('is-routing');
    }, 550);
  }

  function pickNextColor(activeWorker) {
    const counts = { green: 0, blue: 0, yellow: 0, red: 0 };
    workers.forEach((w) => {
      if (w !== activeWorker && counts[w.color] !== undefined) {
        counts[w.color]++;
      }
    });
    const underTwo = COLORS.filter((c) => counts[c] < 2);
    const diffColor = underTwo.filter((c) => c !== activeWorker.color);
    const pool = diffColor.length ? diffColor : (underTwo.length ? underTwo : COLORS);
    return pool[Math.floor(Math.random() * pool.length)];
  }

  function pickNextDotIndex() {
    const occupied = new Set(workers.map((w) => w.dotIndex));
    const maxDots = window.innerWidth <= 640 ? Math.min(234, dots.length) : dots.length;
    for (let attempt = 0; attempt < 40; attempt++) {
      const candidate = Math.floor(Math.random() * maxDots);
      if (!occupied.has(candidate)) return candidate;
    }
    return 0;
  }

  function renderDotState(dotIdx, state, color, actorName) {
    const dot = dots[dotIdx];
    if (!dot) return;
    dot.className = 'as-viz__dot';
    if (state === 'suspended') {
      dot.classList.add('as-viz__dot--snapshot');
      dot.removeAttribute('data-actor-color');
      dot.setAttribute('data-actor-state', 'suspended');
      dot.removeAttribute('title');
      return;
    }
    dot.classList.add(`as-viz__dot--${state}`);
    if (state === 'resuming') dot.classList.add('pulse');
    dot.setAttribute('data-actor-color', color);
    dot.setAttribute('data-actor-state', state);
    dot.setAttribute('title', `actor/${actorName} (${state})`);
  }

  function renderWorkerState(w) {
    const { el, nameEl, subEl, dotEl, barEl, state, color, actorName, width, latencyMs, resumeMs, suspendMs } = w;
    el.className = `as-viz__pod as-viz__pod--${state}`;
    el.setAttribute('data-dot-index', String(w.dotIndex));
    el.setAttribute('data-actor-color', color);
    el.setAttribute('data-actor-state', state);

    if (dotEl) {
      dotEl.className = `as-viz__pod-dot as-viz__pod-dot--${state}${state === 'resuming' ? ' pulse' : ''}`;
    }
    if (nameEl) {
      nameEl.textContent = `actor/${actorName}`;
    }
    if (subEl) {
      if (state === 'resuming') {
        subEl.textContent = `↻ resuming · ${latencyMs || 284}ms`;
      } else if (state === 'suspending') {
        subEl.textContent = '⏸ suspending state';
      } else {
        subEl.textContent = 'running in sandbox';
      }
    }
    if (barEl) {
      barEl.className = `as-viz__pod-bar as-viz__pod-bar--${state}`;
      if (state === 'running') {
        barEl.style.width = `${width}%`;
      } else if (state === 'resuming') {
        barEl.style.setProperty('--phase-ms', `${resumeMs || 900}ms`);
        barEl.style.width = '100%';
      } else {
        barEl.style.setProperty('--phase-ms', `${suspendMs || 700}ms`);
        barEl.style.width = '0%';
      }
    }
  }

  function updateHeaderCounts() {
    let running = 0;
    let resuming = 0;
    let suspending = 0;
    workers.forEach((w) => {
      if (w.state === 'running') running++;
      else if (w.state === 'resuming') resuming++;
      else if (w.state === 'suspending') suspending++;
    });
    if (runningCountEl) runningCountEl.textContent = String(running);
    if (resumingCountEl) resumingCountEl.textContent = String(resuming);
    if (suspendingCountEl) suspendingCountEl.textContent = String(suspending);
  }

  let prefixCursor = 0;
  setInterval(() => {
    const t = performance.now();
    let changed = false;

    workers.forEach((w) => {
      if (t < w.nextTransitionAt) {
        if (w.state === 'running' && Math.random() < 0.25 && w.barEl) {
          const delta = Math.floor(Math.random() * 17) - 8;
          w.width = Math.max(46, Math.min(94, w.width + delta));
          w.barEl.style.width = `${w.width}%`;
        }
        return;
      }

      if (w.state === 'running') {
        w.state = 'suspending';
        w.nextTransitionAt = t + w.suspendMs;
        renderDotState(w.dotIndex, 'suspending', w.color, w.actorName);
        renderWorkerState(w);
        changed = true;
      } else if (w.state === 'suspending') {
        // Old actor becomes suspended (0 pods) in the 250-dot grid
        renderDotState(w.dotIndex, 'suspended', w.color, w.actorName);

        // Pick next cycle duration from [2000, 3000, 5000, 6000] ms (divisors of 30s)
        const nextCycleMs = CYCLE_DURATIONS_MS[Math.floor(Math.random() * CYCLE_DURATIONS_MS.length)];
        const split = PHASE_SPLITS[nextCycleMs];
        w.cycleMs = nextCycleMs;
        w.resumeMs = split.resumeMs;
        w.runMs = split.runMs;
        w.suspendMs = split.suspendMs;

        // A new random actor wakes up in the 250-dot grid and resumes onto this worker
        const newDotIndex = pickNextDotIndex();
        const newColor = pickNextColor(w);
        const prefix = WORKLOAD_PREFIXES[prefixCursor % WORKLOAD_PREFIXES.length];
        prefixCursor++;

        w.dotIndex = newDotIndex;
        w.color = newColor;
        w.actorName = `${prefix}-${newDotIndex}`;
        w.state = 'resuming';
        w.latencyMs = 210 + Math.floor(Math.random() * 175);
        w.width = 54 + Math.floor(Math.random() * 38);
        w.nextTransitionAt = t + w.resumeMs;

        renderDotState(w.dotIndex, 'resuming', w.color, w.actorName);
        renderWorkerState(w);
        pulseRouter(w.color);
        changed = true;
      } else if (w.state === 'resuming') {
        w.state = 'running';
        w.nextTransitionAt = t + w.runMs;
        renderDotState(w.dotIndex, 'running', w.color, w.actorName);
        renderWorkerState(w);
        changed = true;
      }
    });

    if (changed) {
      updateHeaderCounts();
    }
  }, 150);
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
