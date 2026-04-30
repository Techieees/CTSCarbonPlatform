/**
 * Enemy type definitions and factory helpers.
 */

export const ENEMY_TYPES = {
  normal: {
    id: "normal",
    name: "Scout",
    hp: 42,
    speed: 1.15,
    reward: 9,
    color: "#5ce38a",
    coreColor: "#8fffb3",
    radius: 0.34,
  },
  fast: {
    id: "fast",
    name: "Racer",
    hp: 24,
    speed: 2.35,
    reward: 11,
    color: "#5cb8ff",
    coreColor: "#9fdcffff",
    radius: 0.28,
  },
  tank: {
    id: "tank",
    name: "Bulwark",
    hp: 130,
    speed: 0.52,
    reward: 24,
    color: "#d4a82c",
    coreColor: "#ffe566",
    radius: 0.44,
  },
  boss: {
    id: "boss",
    name: "Overlord",
    hp: 720,
    speed: 0.42,
    reward: 160,
    color: "#ff2d6e",
    coreColor: "#ff8fb8",
    radius: 0.52,
  },
};

/**
 * @param {string} typeId
 * @param {number} hpMul
 * @param {number} speedMul
 * @param {number} rewardMul
 */
export function createEnemy(typeId, hpMul, speedMul, rewardMul) {
  const def = ENEMY_TYPES[typeId] || ENEMY_TYPES.normal;
  return {
    typeId: def.id,
    name: def.name,
    maxHp: Math.max(1, Math.round(def.hp * hpMul)),
    hp: Math.max(1, Math.round(def.hp * hpMul)),
    speed: def.speed * speedMul,
    reward: Math.max(1, Math.round(def.reward * rewardMul)),
    color: def.color,
    coreColor: def.coreColor,
    radius: def.radius,
    segIndex: 0,
    t: 0,
    slowMul: 1,
    slowTime: 0,
  };
}

export function getEnemyLabel(typeId) {
  const d = ENEMY_TYPES[typeId];
  return d ? d.name : typeId;
}
