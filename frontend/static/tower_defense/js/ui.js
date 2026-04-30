/**
 * DOM wiring: menus, HUD, level grid, tower tray.
 */

import { Game, GameState } from "./game_engine.js";
import { TOWER_TYPES, upgradeGoldCost, towerSellRefund } from "./towers.js";

function $(id) {
  const el = document.getElementById(id);
  if (!el) throw new Error("Missing #" + id);
  return el;
}

function showOverlay(show, title, sub, buttons) {
  const ov = $("overlay-menu");
  const tit = $("menu-title");
  const subEl = $("menu-sub");
  const stack = $("menu-buttons");
  if (show) {
    ov.classList.remove("hidden");
    tit.textContent = title;
    subEl.textContent = sub || "";
    stack.innerHTML = "";
    for (const b of buttons) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = b.primary ? "btn btn-primary" : "btn btn-ghost";
      btn.textContent = b.label;
      btn.addEventListener("click", b.onClick);
      stack.appendChild(btn);
    }
  } else {
    ov.classList.add("hidden");
  }
}

function syncMainLayout(game) {
  const welcome = $("hud-welcome");
  const playing = $("hud-playing");
  const levelSel = $("hud-level-select");
  const s = game.state;
  welcome.classList.toggle("hidden", s !== GameState.MENU);
  levelSel.classList.toggle("hidden", s !== GameState.LEVEL_SELECT);
  const playHud =
    s === GameState.PLAYING ||
    s === GameState.PAUSED ||
    s === GameState.GAME_OVER ||
    s === GameState.VICTORY;
  playing.classList.toggle("hidden", !playHud);
}

function buildTowerTray(game, selectedId) {
  const tray = $("tower-tray");
  tray.innerHTML = "";
  for (const id of Object.keys(TOWER_TYPES)) {
    const def = TOWER_TYPES[id];
    const b = document.createElement("button");
    b.type = "button";
    b.className = "tower-btn" + (selectedId === id ? " active" : "");
    b.innerHTML = `<span>${def.name}</span><span class="cost">${def.cost}¢</span>`;
    b.title = `${def.name} — rng ${def.range.toFixed(1)}, dmg ${def.damage}, cd ${def.cooldown}s`;
    b.addEventListener("click", () => {
      game.setPlaceType(id);
      buildTowerTray(game, id);
    });
    tray.appendChild(b);
  }
}

function buildLevelGrid(game) {
  const grid = $("level-grid");
  grid.innerHTML = "";
  const unlocked = game.getUnlockedMax();
  for (let i = 1; i <= 20; i++) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "level-cell" + (i <= unlocked ? " unlocked" : " locked");
    const hi = game.getHighScore(i);
    btn.innerHTML = `<span>${i}</span>${hi ? `<span class="hi">Hi ${hi}</span>` : ""}`;
    btn.disabled = i > unlocked;
    btn.addEventListener("click", () => {
      if (i > unlocked) return;
      showOverlay(false, "", "", []);
      game.loadLevel(i);
      buildTowerTray(game, game.placeTowerType || "basic");
      syncMainLayout(game);
      refreshHud(game);
    });
    grid.appendChild(btn);
  }
}

function refreshHud(game) {
  syncMainLayout(game);
  const coins = $("stat-coins");
  const lives = $("stat-lives");
  const wave = $("stat-wave");
  const waveMax = $("stat-wave-max");
  const lvl = $("stat-level");
  const preview = $("wave-preview");
  const selInfo = $("selection-info");
  const actions = $("tower-actions");
  const btnUp = $("btn-upgrade");
  const btnSell = $("btn-sell");

  if (game.level) {
    coins.textContent = String(Math.floor(game.coins));
    lives.textContent = String(game.lives);
    wave.textContent = String(Math.min(game.waveIndex + 1, game.level.waves.length));
    waveMax.textContent = String(game.level.waves.length);
    lvl.textContent = `${game.levelId} (${game.level.diffLabel})`;
    if (game.waveIndex >= game.level.waves.length && !game.waveRunning && game.state === GameState.PLAYING) {
      preview.textContent = "All waves cleared.";
    } else if (game.waveRunning) {
      preview.textContent = "Wave in progress…";
    } else {
      preview.textContent = game.getNextWavePreviewText();
    }
  } else {
    coins.textContent = "—";
    lives.textContent = "—";
    wave.textContent = "—";
    waveMax.textContent = "—";
    lvl.textContent = "—";
    preview.textContent = "—";
  }

  $("btn-start-wave").toggleAttribute(
    "disabled",
    !game.level || game.waveRunning || game.state !== GameState.PLAYING || game.waveIndex >= (game.level?.waves.length || 0)
  );
  $("btn-pause").textContent = game.state === GameState.PAUSED ? "Resume" : "Pause";
  $("btn-pause").toggleAttribute(
    "disabled",
    !game.level || game.state === GameState.GAME_OVER || game.state === GameState.VICTORY
  );
  $("btn-debug").textContent = game.debugMode ? "Debug on" : "Debug off";

  const tw = game.selectedTower;
  if (tw && game.state === GameState.PLAYING) {
    const uc = upgradeGoldCost(tw.baseDef, tw.tier);
    selInfo.textContent = `${tw.baseDef.name} (Lv.${tw.tier}) — sell +${towerSellRefund(tw)}¢`;
    actions.classList.remove("hidden");
    btnUp.textContent = uc != null ? `Upgrade (${uc}¢)` : "Max level";
    btnUp.disabled = uc == null || game.coins < uc;
    btnSell.disabled = false;
  } else {
    selInfo.textContent = "Click a tower on the map to upgrade or sell.";
    actions.classList.add("hidden");
  }

  if (game.state === GameState.PAUSED) {
    showOverlay(true, "Paused", "Tactical pause", [
      {
        label: "Resume",
        primary: true,
        onClick: () => {
          game.pauseToggle();
          showOverlay(false, "", "", []);
          refreshHud(game);
        },
      },
      {
        label: "Quit to menu",
        onClick: () => {
          if (game.state === GameState.PAUSED) game.pauseToggle();
          showOverlay(false, "", "", []);
          game.goMenu();
          refreshHud(game);
        },
      },
    ]);
  } else if (game.state === GameState.GAME_OVER) {
    showOverlay(true, "GAME OVER", "Core breached.", [
      {
        label: "Restart sector",
        primary: true,
        onClick: () => {
          showOverlay(false, "", "", []);
          game.loadLevel(game.levelId);
          refreshHud(game);
        },
      },
      {
        label: "Level select",
        onClick: () => {
          showOverlay(false, "", "", []);
          game.goLevelSelect();
          buildLevelGrid(game);
          refreshHud(game);
        },
      },
      {
        label: "Main menu",
        onClick: () => {
          showOverlay(false, "", "", []);
          game.goMenu();
          refreshHud(game);
        },
      },
    ]);
  } else if (game.state === GameState.VICTORY) {
    const sc = game.getHighScore(game.levelId);
    showOverlay(true, "SECTOR CLEAR", `High score stored: ${sc}`, [
      {
        label: "Next sector",
        primary: true,
        onClick: () => {
          showOverlay(false, "", "", []);
          if (game.levelId < 20) {
            game.loadLevel(game.levelId + 1);
            buildTowerTray(game, game.placeTowerType || "basic");
          } else {
            game.goLevelSelect();
            buildLevelGrid(game);
          }
          refreshHud(game);
        },
      },
      {
        label: "Level select",
        onClick: () => {
          showOverlay(false, "", "", []);
          game.goLevelSelect();
          buildLevelGrid(game);
          refreshHud(game);
        },
      },
    ]);
  }
}

function bindCanvas(game, canvas) {
  canvas.addEventListener("mousemove", (e) => {
    const r = canvas.getBoundingClientRect();
    const mx = e.clientX - r.left;
    const my = e.clientY - r.top;
    game.handleMove(mx, my);
  });
  canvas.addEventListener("mouseleave", () => {
    game.hoverCell = null;
    game.onHud && game.onHud();
  });
  canvas.addEventListener("click", (e) => {
    const r = canvas.getBoundingClientRect();
    const mx = e.clientX - r.left;
    const my = e.clientY - r.top;
    game.handleClick(mx, my);
    refreshHud(game);
  });
}

document.addEventListener("DOMContentLoaded", () => {
  const canvas = /** @type {HTMLCanvasElement} */ ($("game-canvas"));
  const game = new Game(canvas);
  game.onHud = () => refreshHud(game);
  game.placeTowerType = "basic";

  bindCanvas(game, canvas);

  $("btn-menu-play").addEventListener("click", () => {
    game.goLevelSelect();
    buildLevelGrid(game);
    refreshHud(game);
  });

  $("btn-back-menu").addEventListener("click", () => {
    game.goMenu();
    refreshHud(game);
  });

  $("btn-start-wave").addEventListener("click", () => {
    game.startWave();
    refreshHud(game);
  });

  $("btn-pause").addEventListener("click", () => {
    if (!game.level) return;
    game.pauseToggle();
    if (game.state !== GameState.PAUSED) {
      showOverlay(false, "", "", []);
    }
    refreshHud(game);
  });

  $("btn-restart").addEventListener("click", () => {
    game.restartLevel();
    showOverlay(false, "", "", []);
    refreshHud(game);
  });

  $("btn-debug").addEventListener("click", () => {
    game.toggleDebug();
    refreshHud(game);
  });

  $("btn-upgrade").addEventListener("click", () => {
    game.upgradeSelected();
    refreshHud(game);
  });

  $("btn-sell").addEventListener("click", () => {
    game.sellSelected();
    refreshHud(game);
  });

  game.state = GameState.MENU;
  syncMainLayout(game);
  game.startLoop();
  refreshHud(game);
});
