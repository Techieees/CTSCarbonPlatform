/**
 * Core engine: grid, loop, waves, rendering, input.
 */

import { createEnemy } from "./enemies.js";
import {
  TOWER_TYPES,
  createTower,
  createProjectile,
  updateProjectile,
  computeSupportBuffs,
  refreshTowerDerived,
  upgradeGoldCost,
  towerSellRefund,
} from "./towers.js";
import { GRID_SIZE, getLevel } from "./levels.js";

export const GameState = {
  MENU: "menu",
  LEVEL_SELECT: "level_select",
  PLAYING: "playing",
  PAUSED: "paused",
  GAME_OVER: "game_over",
  VICTORY: "victory",
};

const LS_UNLOCK = "neonTd_unlocked";
const LS_SCORES = "neonTd_highscores";

function keyCell(gx, gy) {
  return `${gx},${gy}`;
}

function dist(ax, ay, bx, by) {
  const dx = ax - bx;
  const dy = ay - by;
  return Math.hypot(dx, dy);
}

export class Game {
  /**
   * @param {HTMLCanvasElement} canvas
   */
  constructor(canvas) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d");
    this.ctx.imageSmoothingEnabled = false;

    /** @type {'menu'|'level_select'|'playing'|'paused'|'game_over'|'victory'} */
    this.state = GameState.MENU;
    this.tileSize = 28;
    this.gridSize = GRID_SIZE;

    this.level = null;
    /** @type {number[][]} */
    this.pathCells = [];
    /** @type {Set<string>} */
    this.pathSet = new Set();

    this.coins = 0;
    this.lives = 20;
    this.waveIndex = 0;
    this.waveRunning = false;
    /** @type {string[]} */
    this._spawnQueue = [];
    this._spawnInterval = 0.35;
    this._spawnCooldown = 0;

    /** @type {ReturnType<createEnemy>[]} */
    this.enemies = [];
    /** @type {ReturnType<createTower>[]} */
    this.towers = [];
    /** @type {ReturnType<createProjectile>[]} */
    this.projectiles = [];

    /** @type {{ gx: number; gy: number } | null} */
    this.hoverCell = null;
    this.hoverPixel = { x: 0, y: 0 };

    /** @type {string | null} */
    this.placeTowerType = "basic";
    /** @type {ReturnType<createTower> | null} */
    this.selectedTower = null;

    this.levelId = 1;
    this._lastTs = 0;
    this._running = false;

    /** @type {((g: Game) => void) | null} */
    this.onHud = null;
  }

  /** @param {number} levelId */
  loadLevel(levelId) {
    this.levelId = Math.max(1, Math.min(20, levelId | 0));
    this.level = getLevel(this.levelId);
    this.pathCells = this.level.pathCells;
    this.pathSet.clear();
    for (const c of this.pathCells) {
      this.pathSet.add(keyCell(c[0], c[1]));
    }
    this.coins = this.level.startCoins;
    this.lives = this.level.startLives;
    this.waveIndex = 0;
    this.waveRunning = false;
    this._spawnQueue = [];
    this.enemies.length = 0;
    this.towers.length = 0;
    this.projectiles.length = 0;
    this.selectedTower = null;
    this.state = GameState.PLAYING;
    this._notifyHud();
  }

  restartLevel() {
    if (!this.level) return;
    this.loadLevel(this.levelId);
  }

  goMenu() {
    this.state = GameState.MENU;
    this.level = null;
    this.waveRunning = false;
    this._notifyHud();
  }

  goLevelSelect() {
    this.state = GameState.LEVEL_SELECT;
    this.level = null;
    this._notifyHud();
  }

  pauseToggle() {
    if (this.state === GameState.PLAYING) {
      this.state = GameState.PAUSED;
    } else if (this.state === GameState.PAUSED) {
      this.state = GameState.PLAYING;
    }
    this._notifyHud();
  }

  getUnlockedMax() {
    const raw = localStorage.getItem(LS_UNLOCK);
    const n = raw ? parseInt(raw, 10) : 1;
    return Number.isFinite(n) ? Math.max(1, Math.min(20, n)) : 1;
  }

  setUnlocked(levelId) {
    const cur = this.getUnlockedMax();
    if (levelId > cur) {
      localStorage.setItem(LS_UNLOCK, String(Math.min(20, levelId)));
    }
  }

  /**
   * @param {number} levelId
   * @param {number} score
   */
  saveHighScore(levelId, score) {
    let map = {};
    try {
      map = JSON.parse(localStorage.getItem(LS_SCORES) || "{}");
    } catch {
      map = {};
    }
    const prev = map[String(levelId)] | 0;
    if (score > prev) {
      map[String(levelId)] = score;
      localStorage.setItem(LS_SCORES, JSON.stringify(map));
    }
  }

  getHighScore(levelId) {
    try {
      const map = JSON.parse(localStorage.getItem(LS_SCORES) || "{}");
      return map[String(levelId)] | 0;
    } catch {
      return 0;
    }
  }

  startWave() {
    if (!this.level || this.state !== GameState.PLAYING) return;
    if (this.waveRunning) return;
    if (this.waveIndex >= this.level.waves.length) return;
    const wave = this.level.waves[this.waveIndex];
    this._spawnQueue = [];
    for (const g of wave.groups) {
      for (let i = 0; i < g.count; i++) {
        this._spawnQueue.push(g.type);
      }
    }
    this._spawnInterval = wave.spawnInterval;
    this._spawnCooldown = 0.35;
    this.waveRunning = true;
    this._notifyHud();
  }

  /** @param {string} typeId */
  spawnEnemy(typeId) {
    if (!this.level) return;
    const e = createEnemy(
      typeId,
      this.level.hpMult,
      this.level.speedMult,
      this.level.rewardMult
    );
    e.segIndex = 0;
    e.t = 0;
    const pos = this._enemyPos(e);
    e.gx = pos.gx;
    e.gy = pos.gy;
    this.enemies.push(e);
  }

  /** @param {ReturnType<createEnemy>} e */
  _enemyPos(e) {
    const p = this.pathCells;
    if (p.length < 2) return { gx: 0.5, gy: 0.5 };
    const a = p[e.segIndex];
    const b = p[e.segIndex + 1];
    const gx = a[0] + 0.5 + (b[0] - a[0]) * e.t;
    const gy = a[1] + 0.5 + (b[1] - a[1]) * e.t;
    return { gx, gy };
  }

  /** @param {number} dt */
  _updateEnemies(dt) {
    const p = this.pathCells;
    if (p.length < 2) return;
    const segCount = p.length - 1;
    for (const e of this.enemies) {
      if (e.hp <= 0) continue;
      if (e.slowTime > 0) {
        e.slowTime -= dt;
        e.slowMul = 0.55;
      } else {
        e.slowMul = 1;
      }
      while (e.segIndex < segCount) {
        const a = p[e.segIndex];
        const b = p[e.segIndex + 1];
        const segLen = Math.abs(b[0] - a[0]) + Math.abs(b[1] - a[1]) || 1;
        const step = ((e.speed * e.slowMul) / segLen) * dt;
        e.t += step;
        if (e.t < 1) break;
        e.t -= 1;
        e.segIndex++;
      }
      if (e.segIndex >= segCount) {
        e._leaked = true;
        continue;
      }
      const pos = this._enemyPos(e);
      e.gx = pos.gx;
      e.gy = pos.gy;
    }

    const kept = [];
    for (const e of this.enemies) {
      if (e._leaked) {
        this.lives -= 1;
        continue;
      }
      if (e.hp <= 0) {
        this.coins += e.reward;
        continue;
      }
      kept.push(e);
    }
    this.enemies = kept;
    if (this.lives <= 0) {
      this._gameOver();
    }
  }

  _gameOver() {
    this.state = GameState.GAME_OVER;
    this.waveRunning = false;
    this._notifyHud();
  }

  _victory() {
    this.state = GameState.VICTORY;
    this.waveRunning = false;
    const score =
      this.levelId * 12000 + this.coins * 12 + this.lives * 800 + this.waveIndex * 400;
    this.saveHighScore(this.levelId, score);
    this.setUnlocked(this.levelId + 1);
    this._notifyHud();
  }

  /** @param {number} dt */
  _waveLogic(dt) {
    if (!this.level || this.state !== GameState.PLAYING) return;
    if (!this.waveRunning) {
      return;
    }
    if (this._spawnQueue.length > 0) {
      this._spawnCooldown -= dt;
      if (this._spawnCooldown <= 0) {
        const ty = this._spawnQueue.shift();
        if (ty) this.spawnEnemy(ty);
        this._spawnCooldown = this._spawnInterval;
      }
    } else if (this.enemies.length === 0) {
      this.waveRunning = false;
      this.waveIndex++;
      if (this.waveIndex >= this.level.waves.length) {
        this._victory();
      } else {
        this._notifyHud();
      }
    }
  }

  /** @param {number} gx @param {number} gy */
  gridToPixel(gx, gy) {
    return { x: gx * this.tileSize, y: gy * this.tileSize };
  }

  /** @param {number} mx @param {number} my */
  pixelToGrid(mx, my) {
    const gx = Math.floor(mx / this.tileSize);
    const gy = Math.floor(my / this.tileSize);
    return { gx, gy };
  }

  /** @param {number} mx @param {number} my */
  handleMove(mx, my) {
    this.hoverPixel = { x: mx, y: my };
    const g = this.pixelToGrid(mx, my);
    if (g.gx >= 0 && g.gx < this.gridSize && g.gy >= 0 && g.gy < this.gridSize) {
      this.hoverCell = g;
    } else {
      this.hoverCell = null;
    }
  }

  /** @param {number} mx @param {number} my */
  handleClick(mx, my) {
    if (this.state !== GameState.PLAYING && this.state !== GameState.PAUSED) return;
    if (this.state === GameState.PAUSED) return;
    const { gx, gy } = this.pixelToGrid(mx, my);
    if (gx < 0 || gx >= this.gridSize || gy < 0 || gy >= this.gridSize) return;

    const tw = this.towers.find((t) => t.gx === gx && t.gy === gy);
    if (tw) {
      this.selectedTower = tw;
      this._notifyHud();
      return;
    }

    if (this.placeTowerType) {
      this.tryPlaceTower(this.placeTowerType, gx, gy);
    }
  }

  /** @param {string} typeId @param {number} gx @param {number} gy */
  tryPlaceTower(typeId, gx, gy) {
    if (!TOWER_TYPES[typeId]) return;
    if (this.pathSet.has(keyCell(gx, gy))) return;
    if (this.towers.some((t) => t.gx === gx && t.gy === gy)) return;
    const def = TOWER_TYPES[typeId];
    if (this.coins < def.cost) return;
    const tower = createTower(typeId, gx, gy);
    if (!tower) return;
    this.coins -= def.cost;
    this.towers.push(tower);
    this.selectedTower = tower;
    this._notifyHud();
  }

  upgradeSelected() {
    const tw = this.selectedTower;
    if (!tw || !this.level) return;
    const cost = upgradeGoldCost(tw.baseDef, tw.tier);
    if (cost == null || this.coins < cost) return;
    this.coins -= cost;
    tw.invested += cost;
    tw.tier++;
    refreshTowerDerived(tw);
    this._notifyHud();
  }

  sellSelected() {
    const tw = this.selectedTower;
    if (!tw) return;
    const idx = this.towers.indexOf(tw);
    if (idx >= 0) {
      this.coins += towerSellRefund(tw);
      this.towers.splice(idx, 1);
    }
    this.selectedTower = null;
    this._notifyHud();
  }

  setPlaceType(typeId) {
    this.placeTowerType = typeId;
    this._notifyHud();
  }

  /** @param {ReturnType<createTower>} tower @param {object | null} enemy */
  _towerShoot(tower, enemy) {
    if (!enemy) return;
    const from = this.gridToPixel(tower.gx + 0.5, tower.gy + 0.5);
    const dmg = tower.damage * (tower._buffD || 1);
    const cd = tower.cooldown / (tower._buffR || 1);
    const ref = { current: enemy };
    if (tower.baseDef.kind === "splash") {
      this.projectiles.push(
        createProjectile(tower, from, ref, dmg, tower.baseDef.projectileSpeed, "splash", tower.splashRadius || 1.2)
      );
    } else {
      this.projectiles.push(
        createProjectile(tower, from, ref, dmg, tower.baseDef.projectileSpeed, "single", 0)
      );
    }
    tower.cooldownLeft = cd;
  }

  _pickTarget(tower) {
    let best = null;
    let bestProg = -1;
    const cx = tower.gx + 0.5;
    const cy = tower.gy + 0.5;
    const r = tower.range;
    for (const e of this.enemies) {
      if (e.hp <= 0) continue;
      if (dist(cx, cy, e.gx, e.gy) > r) continue;
      const prog = e.segIndex + e.t;
      if (prog > bestProg) {
        bestProg = prog;
        best = e;
      }
    }
    return best;
  }

  /** @param {number} dt */
  _updateTowers(dt) {
    computeSupportBuffs(this);
    for (const tower of this.towers) {
      if (tower.baseDef.kind === "support") continue;
      tower.cooldownLeft = Math.max(0, tower.cooldownLeft - dt);
      if (tower.cooldownLeft > 0) continue;
      const tgt = this._pickTarget(tower);
      if (tgt) this._towerShoot(tower, tgt);
    }

    const nextP = [];
    for (const p of this.projectiles) {
      updateProjectile(this, p, dt, this.enemies);
      if (!p.dead) nextP.push(p);
    }
    this.projectiles = nextP;
  }

  _notifyHud() {
    if (this.onHud) this.onHud(this);
  }

  /** @param {number} timestamp */
  tick(timestamp) {
    if (!this._running) return;
    if (!this._lastTs) this._lastTs = timestamp;
    let dt = (timestamp - this._lastTs) / 1000;
    this._lastTs = timestamp;
    if (dt > 0.05) dt = 0.05;

    if (this.state === GameState.PLAYING) {
      this._waveLogic(dt);
      this._updateEnemies(dt);
      this._updateTowers(dt);
    }

    this.render();
    requestAnimationFrame((t) => this.tick(t));
  }

  startLoop() {
    if (this._running) return;
    this._running = true;
    this._lastTs = 0;
    requestAnimationFrame((t) => this.tick(t));
  }

  render() {
    const ctx = this.ctx;
    const ts = this.tileSize;
    const W = this.gridSize * ts;
    const H = this.gridSize * ts;

    if (!this.level) {
      ctx.fillStyle = "#06080d";
      ctx.fillRect(0, 0, W, H);
      ctx.fillStyle = "rgba(0, 255, 200, 0.12)";
      ctx.font = "11px monospace";
      ctx.fillText("20 × 20 GRID — select a sector", 14, 28);
      ctx.fillStyle = "rgba(122, 138, 163, 0.5)";
      ctx.font = "10px monospace";
      ctx.fillText("NEON GRID TD", 14, 46);
      return;
    }
    ctx.fillStyle = "#06080d";
    ctx.fillRect(0, 0, W, H);

    for (let y = 0; y < this.gridSize; y++) {
      for (let x = 0; x < this.gridSize; x++) {
        const px = x * ts;
        const py = y * ts;
        const onPath = this.pathSet.has(keyCell(x, y));
        if (onPath) {
          ctx.fillStyle = "#1a2233";
        } else {
          ctx.fillStyle = (x + y) % 2 === 0 ? "#0f1812" : "#0c1610";
        }
        ctx.fillRect(px, py, ts, ts);
        ctx.strokeStyle = "rgba(0,255,200,0.06)";
        ctx.strokeRect(px + 0.5, py + 0.5, ts - 1, ts - 1);
      }
    }

    for (let i = 1; i < this.pathCells.length; i++) {
      const a = this.pathCells[i - 1];
      const b = this.pathCells[i];
      const ax = (a[0] + 0.5) * ts;
      const ay = (a[1] + 0.5) * ts;
      const bx = (b[0] + 0.5) * ts;
      const by = (b[1] + 0.5) * ts;
      ctx.strokeStyle = "rgba(0,255,200,0.22)";
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(ax, ay);
      ctx.lineTo(bx, by);
      ctx.stroke();
    }

    ctx.lineWidth = 1;
    for (const tw of this.towers) {
      const px = tw.gx * ts;
      const py = tw.gy * ts;
      ctx.fillStyle = "rgba(0,0,0,0.35)";
      ctx.fillRect(px + 3, py + 3, ts - 6, ts - 6);
      ctx.strokeStyle = tw.baseDef.color;
      ctx.strokeRect(px + 4, py + 4, ts - 8, ts - 8);
      ctx.fillStyle = tw.baseDef.color;
      const s = ts * 0.22;
      ctx.fillRect(px + ts / 2 - s / 2, py + ts / 2 - s / 2, s, s);
      if (tw.tier > 1) {
        ctx.fillStyle = "#fff";
        ctx.font = "10px monospace";
        ctx.fillText(String(tw.tier), px + 6, py + 12);
      }
    }

    const showRange =
      this.hoverCell &&
      this.state === GameState.PLAYING &&
      !this.pathSet.has(keyCell(this.hoverCell.gx, this.hoverCell.gy));

    if (showRange) {
      const hx = this.hoverCell.gx;
      const hy = this.hoverCell.gy;
      const occ = this.towers.some((t) => t.gx === hx && t.gy === hy);
      if (!occ && this.placeTowerType && TOWER_TYPES[this.placeTowerType]) {
        const r = TOWER_TYPES[this.placeTowerType].range * ts;
        const cx = (hx + 0.5) * ts;
        const cy = (hy + 0.5) * ts;
        ctx.beginPath();
        ctx.arc(cx, cy, r, 0, Math.PI * 2);
        ctx.strokeStyle = "rgba(255,0,170,0.35)";
        ctx.fillStyle = "rgba(255,0,170,0.06)";
        ctx.fill();
        ctx.stroke();
      }
    }

    if (this.selectedTower && this.state === GameState.PLAYING) {
      const tw = this.selectedTower;
      const r = tw.range * ts;
      const cx = (tw.gx + 0.5) * ts;
      const cy = (tw.gy + 0.5) * ts;
      ctx.beginPath();
      ctx.arc(cx, cy, r, 0, Math.PI * 2);
      ctx.strokeStyle = "rgba(0,255,200,0.45)";
      ctx.fillStyle = "rgba(0,255,200,0.04)";
      ctx.fill();
      ctx.stroke();
    }

    for (const p of this.projectiles) {
      ctx.fillStyle = p.color;
      ctx.beginPath();
      ctx.arc(p.x, p.y, 3.5, 0, Math.PI * 2);
      ctx.fill();
    }

    for (const e of this.enemies) {
      if (e.hp <= 0) continue;
      const p = this.gridToPixel(e.gx, e.gy);
      const rad = e.radius * ts;
      ctx.fillStyle = e.color;
      ctx.beginPath();
      ctx.arc(p.x, p.y, rad, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = e.coreColor;
      ctx.beginPath();
      ctx.arc(p.x, p.y, rad * 0.45, 0, Math.PI * 2);
      ctx.fill();
      const frac = e.hp / e.maxHp;
      ctx.strokeStyle = "rgba(0,0,0,0.55)";
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(p.x, p.y - rad - 4, 5, 0, Math.PI * 2);
      ctx.stroke();
      ctx.strokeStyle = "#00ffc8";
      ctx.beginPath();
      ctx.arc(p.x, p.y - rad - 4, 5, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * frac);
      ctx.stroke();
      ctx.lineWidth = 1;
    }

    if (this.level && this.pathCells.length) {
      const end = this.pathCells[this.pathCells.length - 1];
      const ex = (end[0] + 0.5) * ts;
      const ey = (end[1] + 0.5) * ts;
      ctx.strokeStyle = "#ff00aa";
      ctx.strokeRect(ex - 8, ey - 8, 16, 16);
      ctx.fillStyle = "rgba(255,0,170,0.15)";
      ctx.fillRect(ex - 8, ey - 8, 16, 16);
    }
  }

  getNextWavePreviewText() {
    if (!this.level) return "—";
    if (this.waveIndex >= this.level.waves.length) return "All waves cleared.";
    const w = this.level.waves[this.waveIndex];
    const parts = [];
    for (const g of w.groups) {
      parts.push(`${g.count}×${g.type}`);
    }
    return parts.join(", ");
  }
}
