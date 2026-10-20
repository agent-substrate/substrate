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

(function () {
  "use strict";

  // Exactly one resource page is visible at a time: the one holding the
  // element the URL hash names, or the first one when nothing matches.
  var resourcePages = document.querySelectorAll(".resource-page");

  function showResourceRoute() {
    var targetId = window.location.hash.slice(1);
    var target = targetId ? document.getElementById(targetId) : null;
    // The hash names either a resource page or something inside one.
    var active = target ? target.closest(".resource-page") : null;
    if (!active && resourcePages.length > 0) {
      active = resourcePages[0];
    }

    resourcePages.forEach(function (page) {
      page.hidden = page !== active;
    });

    // The browser's own fragment scroll misses a target that was hidden
    // until the line above.
    if (target) {
      target.scrollIntoView({ block: "start" });
    }
  }

  window.addEventListener("hashchange", showResourceRoute);
  showResourceRoute();

  // Each "Show fields" toggle reveals the .attr-children block its
  // aria-controls names.
  document.querySelectorAll(".expand-toggle").forEach(function (btn) {
    var target = document.getElementById(btn.getAttribute("aria-controls"));
    var sign = btn.querySelector(".sign");
    btn.addEventListener("click", function () {
      var open = btn.getAttribute("aria-expanded") === "true";
      btn.setAttribute("aria-expanded", String(!open));
      target.hidden = open;
      sign.textContent = open ? "+" : "−";
      btn.lastChild.textContent = " " + (open ? "Show fields" : "Hide fields");
    });
  });

  var index = JSON.parse(document.getElementById("apitool-index").textContent);
  var input = document.getElementById("search-input");
  var results = document.getElementById("search-results");

  var KIND_LABELS = {
    "resource": "Resources",
    "method": "Methods",
    "message": "Messages",
    "field": "Fields",
    "enum": "Enums",
    "enum value": "Enum values"
  };
  var KIND_ORDER = ["resource", "method", "message", "field", "enum", "enum value"];
  var MAX_RESULTS = 40;

  var activeIndex = -1;

  function matches(entry, query) {
    return entry.name.toLowerCase().indexOf(query) !== -1 ||
      entry.fullName.toLowerCase().indexOf(query) !== -1;
  }

  function search(query) {
    query = query.trim().toLowerCase();
    if (!query) {
      return [];
    }
    var out = [];
    for (var i = 0; i < index.length && out.length < MAX_RESULTS; i++) {
      if (matches(index[i], query)) {
        out.push(index[i]);
      }
    }
    return out;
  }

  function render(entries) {
    results.innerHTML = "";
    activeIndex = -1;

    if (entries.length === 0) {
      var empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = "No matches";
      results.appendChild(empty);
      results.hidden = false;
      return;
    }

    var byKind = {};
    entries.forEach(function (e) {
      (byKind[e.kind] = byKind[e.kind] || []).push(e);
    });

    KIND_ORDER.forEach(function (kind) {
      var group = byKind[kind];
      if (!group || group.length === 0) {
        return;
      }
      var label = document.createElement("div");
      label.className = "group-label";
      label.textContent = KIND_LABELS[kind] || kind;
      results.appendChild(label);

      group.forEach(function (entry) {
        var row = document.createElement("div");
        row.className = "result";
        row.dataset.anchor = entry.anchor;

        var name = document.createElement("span");
        name.className = "mono";
        name.textContent = entry.name;
        row.appendChild(name);

        var full = document.createElement("span");
        full.className = "full-name mono";
        full.textContent = entry.fullName;
        full.title = entry.fullName;
        row.appendChild(full);

        row.addEventListener("mousedown", function (ev) {
          // mousedown, not click: fires before the input's blur handler
          // would otherwise hide the dropdown first.
          ev.preventDefault();
          navigate(entry.anchor);
        });

        results.appendChild(row);
      });
    });

    results.hidden = false;
  }

  function navigate(anchor) {
    results.hidden = true;
    var el = document.getElementById(anchor);
    if (!el) {
      return;
    }
    if (window.location.hash === "#" + anchor) {
      el.scrollIntoView({ block: "start" });
    } else {
      window.location.hash = anchor;
    }
  }

  function setActive(i) {
    var rows = results.querySelectorAll(".result");
    if (activeIndex >= 0 && rows[activeIndex]) {
      rows[activeIndex].classList.remove("active");
    }
    activeIndex = i;
    if (activeIndex >= 0 && rows[activeIndex]) {
      rows[activeIndex].classList.add("active");
      rows[activeIndex].scrollIntoView({ block: "nearest" });
    }
  }

  input.addEventListener("input", function () {
    if (!input.value.trim()) {
      results.innerHTML = "";
      results.hidden = true;
      return;
    }
    render(search(input.value));
  });

  input.addEventListener("keydown", function (ev) {
    var rows = results.querySelectorAll(".result");
    if (ev.key === "ArrowDown") {
      ev.preventDefault();
      if (rows.length > 0) {
        setActive((activeIndex + 1) % rows.length);
      }
    } else if (ev.key === "ArrowUp") {
      ev.preventDefault();
      if (rows.length > 0) {
        setActive((activeIndex - 1 + rows.length) % rows.length);
      }
    } else if (ev.key === "Enter") {
      var row = rows[Math.max(activeIndex, 0)];
      if (row) {
        navigate(row.dataset.anchor);
      }
    } else if (ev.key === "Escape") {
      results.hidden = true;
      input.blur();
    }
  });

  input.addEventListener("focus", function () {
    if (results.querySelector(".result")) {
      results.hidden = false;
    }
  });

  document.addEventListener("click", function (ev) {
    if (ev.target !== input && !results.contains(ev.target)) {
      results.hidden = true;
    }
  });
})();
