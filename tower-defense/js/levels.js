/**
 * Level paths (20x20 grid), waves, and difficulty scaling.
 * Waypoints are [col, row] integers; expandPath fills orthogonal steps.
 */

export const GRID_SIZE = 20;

/** @param {[number,number][]} waypoints */
export function expandPath(waypoints) {
  if (!waypoints.length) return [];
  const out = [[waypoints[0][0], waypoints[0][1]]];
  for (let i = 1; i < waypoints.length; i++) {
    let px = out[out.length - 1][0];
    let py = out[out.length - 1][1];
    const tx = waypoints[i][0];
    const ty = waypoints[i][1];
    while (px !== tx || py !== ty) {
      if (px < tx) px++;
      else if (px > tx) px--;
      else if (py < ty) py++;
      else if (py > ty) py--;
      out.push([px, py]);
    }
  }
  return out;
}

/** Tier 0 easy 1–5, 1 medium 6–10, 2 hard 11–15, 3 extreme 16–20 */
export function tierForLevel(levelId) {
  if (levelId <= 5) return 0;
  if (levelId <= 10) return 1;
  if (levelId <= 15) return 2;
  return 3;
}

function difficultyMultipliers(levelId, tier) {
  const baseHp = 0.72 + levelId * 0.052 + tier * 0.14;
  const baseSp = 0.9 + levelId * 0.011 + tier * 0.028;
  const baseRw = 1.0 + levelId * 0.018 + tier * 0.04;
  return {
    hpMult: Math.min(4.2, baseHp),
    speedMult: Math.min(1.55, baseSp),
    rewardMult: Math.min(2.4, baseRw),
  };
}

function hasBoss(levelId) {
  return levelId % 5 === 0;
}

function buildWaves(levelId, tier) {
  const waveCount = 5 + tier + Math.min(3, Math.floor(levelId / 4));
  /** @type {{ groups: { type: string; count: number }[]; spawnInterval: number }[]} */
  const waves = [];
  for (let w = 0; w < waveCount; w++) {
    const groups = [];
    const n = 5 + w * 2 + tier * 3 + Math.floor(levelId / 2);
    groups.push({ type: "normal", count: Math.min(48, n + 4) });
    if (w >= 1) {
      groups.push({ type: "fast", count: Math.min(22, 2 + Math.floor(w / 2) + tier * 2) });
    }
    if (w >= 2) {
      groups.push({
        type: "tank",
        count: Math.min(8, 1 + Math.floor(w / 3) + tier),
      });
    }
    const spawnInterval = Math.max(0.14, 0.42 - tier * 0.04 - levelId * 0.006);
    waves.push({ groups, spawnInterval });
  }
  if (hasBoss(levelId)) {
    const last = waves[waves.length - 1];
    last.groups.push({ type: "boss", count: 1 });
  }
  return waves;
}

/** 20 distinct waypoint paths (starts/ends on edges) */
const WAYPOINTS = [
  [[0, 10], [19, 10]],
  [[0, 5], [8, 5], [8, 14], [3, 14], [3, 7], [19, 7]],
  [[0, 15], [12, 15], [12, 5], [6, 5], [6, 12], [19, 12]],
  [[0, 3], [15, 3], [15, 17], [2, 17], [2, 9], [19, 9]],
  [[0, 18], [18, 18], [18, 2], [10, 2], [10, 16], [4, 16], [4, 8], [19, 8]],
  [[10, 0], [10, 19]],
  [[0, 14], [16, 14], [16, 4], [7, 4], [7, 17], [19, 17]],
  [[19, 6], [3, 6], [3, 16], [14, 16], [14, 1], [1, 1], [1, 19], [19, 19]],
  [[0, 8], [11, 8], [11, 14], [5, 14], [5, 3], [18, 3], [18, 18], [19, 18]],
  [[0, 19], [9, 19], [9, 0], [19, 0], [19, 11], [4, 11], [4, 16], [15, 16], [15, 6], [19, 6]],
  [[3, 0], [3, 17], [17, 17], [17, 3], [8, 3], [8, 11], [19, 11]],
  [[0, 12], [18, 12], [18, 7], [6, 7], [6, 18], [14, 18], [14, 2], [19, 2]],
  [[19, 15], [1, 15], [1, 5], [12, 5], [12, 19]],
  [[0, 1], [17, 1], [17, 14], [5, 14], [5, 9], [13, 9], [13, 18], [19, 18]],
  [[8, 0], [8, 15], [1, 15], [1, 7], [16, 7], [16, 12], [19, 12]],
  [[0, 17], [19, 17], [19, 0], [2, 0], [2, 11], [15, 11], [15, 4], [10, 4], [10, 19]],
  [[19, 9], [0, 9], [0, 4], [14, 4], [14, 16], [6, 16], [6, 11], [19, 11]],
  [[5, 0], [5, 14], [18, 14], [18, 6], [2, 6], [2, 18], [11, 18], [11, 2], [19, 2]],
  [[0, 6], [13, 6], [13, 19], [7, 19], [7, 2], [19, 2], [19, 15], [4, 15], [4, 9], [16, 9]],
  [[19, 3], [1, 3], [1, 17], [17, 17], [17, 8], [8, 8], [8, 14], [0, 14]],
];

function buildLevel(levelId) {
  const tier = tierForLevel(levelId);
  const { hpMult, speedMult, rewardMult } = difficultyMultipliers(levelId, tier);
  const pathCells = expandPath(WAYPOINTS[levelId - 1] || WAYPOINTS[0]);
  const diffLabel =
    tier === 0 ? "Easy" : tier === 1 ? "Medium" : tier === 2 ? "Hard" : "Extreme";

  return {
    id: levelId,
    name: `Sector ${levelId}`,
    diffLabel,
    tier,
    pathCells,
    waves: buildWaves(levelId, tier),
    startCoins: 380 + levelId * 42 + tier * 55,
    startLives: 22 - Math.min(6, tier * 2),
    hpMult,
    speedMult,
    rewardMult,
  };
}

/** @type {ReturnType<typeof buildLevel>[]} */
const CACHE = [];
for (let i = 1; i <= 20; i++) {
  CACHE.push(buildLevel(i));
}

export const LEVELS = CACHE;

export function getLevel(levelId) {
  const id = Math.max(1, Math.min(20, levelId | 0));
  return LEVELS[id - 1];
}
