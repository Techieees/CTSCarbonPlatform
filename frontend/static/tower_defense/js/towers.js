/**
 * Tower definitions, Tower instances, projectiles, support aura.
 */

export const TOWER_TYPES = {
  basic: {
    id: "basic",
    name: "Pulse",
    cost: 55,
    range: 2.25,
    damage: 13,
    cooldown: 0.52,
    color: "#00ffcc",
    projectileSpeed: 14,
    kind: "single",
  },
  fast: {
    id: "fast",
    name: "Gattling",
    cost: 85,
    range: 1.85,
    damage: 6,
    cooldown: 0.2,
    color: "#fff23a",
    projectileSpeed: 18,
    kind: "single",
  },
  heavy: {
    id: "heavy",
    name: "Siege",
    cost: 130,
    range: 2.05,
    damage: 38,
    cooldown: 1.05,
    color: "#ff7722",
    projectileSpeed: 10,
    kind: "single",
  },
  sniper: {
    id: "sniper",
    name: "Rail",
    cost: 210,
    range: 4.6,
    damage: 85,
    cooldown: 1.75,
    color: "#c86bff",
    projectileSpeed: 28,
    kind: "single",
  },
  splash: {
    id: "splash",
    name: "Nova",
    cost: 155,
    range: 2.35,
    damage: 16,
    cooldown: 0.88,
    splashRadius: 1.35,
    color: "#22d4ff",
    projectileSpeed: 11,
    kind: "splash",
  },
  support: {
    id: "support",
    name: "Relay",
    cost: 105,
    range: 2.6,
    damage: 0,
    cooldown: 999,
    color: "#66ffbb",
    projectileSpeed: 0,
    kind: "support",
    buffDamage: 1.22,
    buffSpeed: 1.18,
  },
};

const UPGRADE_COST_FACTOR = 0.55;
const SELL_RATIO = 0.6;

export function upgradeGoldCost(baseDef, currentTier) {
  if (currentTier >= 3) return null;
  return Math.ceil(baseDef.cost * UPGRADE_COST_FACTOR * (currentTier + 1));
}

export function applyTierStats(baseDef, tier) {
  const t = Math.max(1, Math.min(3, tier));
  const dmgScale = Math.pow(1.18, t - 1);
  const rangeBonus = (t - 1) * 0.18;
  const cdScale = Math.pow(0.9, t - 1);
  if (baseDef.kind === "support") {
    return {
      range: baseDef.range + rangeBonus,
      damage: 0,
      cooldown: 999,
      damageMult: Math.pow(1.08, t - 1) * baseDef.buffDamage,
      speedMult: Math.pow(1.05, t - 1) * baseDef.buffSpeed,
    };
  }
  return {
    range: baseDef.range + rangeBonus,
    damage: baseDef.damage * dmgScale,
    cooldown: baseDef.cooldown * cdScale,
    splashRadius: baseDef.splashRadius ? baseDef.splashRadius + (t - 1) * 0.12 : undefined,
    damageMult: 1,
    speedMult: 1,
  };
}

export function createTower(typeId, gx, gy) {
  const def = TOWER_TYPES[typeId];
  if (!def) return null;
  const s = applyTierStats(def, 1);
  return {
    typeId: def.id,
    gx,
    gy,
    tier: 1,
    invested: def.cost,
    cooldownLeft: 0,
    baseDef: def,
    range: s.range,
    damage: s.damage,
    cooldown: s.cooldown,
    splashRadius: def.splashRadius,
    buffDamage: def.kind === "support" ? s.damageMult : 1,
    buffSpeed: def.kind === "support" ? s.speedMult : 1,
  };
}

export function refreshTowerDerived(tower) {
  const def = tower.baseDef;
  const s = applyTierStats(def, tower.tier);
  tower.range = s.range;
  if (def.kind === "support") {
    tower.damage = 0;
    tower.cooldown = 999;
    tower.buffDamage = s.damageMult;
    tower.buffSpeed = s.speedMult;
  } else {
    tower.damage = s.damage;
    tower.cooldown = s.cooldown;
    tower.buffDamage = 1;
    tower.buffSpeed = 1;
    if (def.splashRadius) {
      tower.splashRadius = def.splashRadius + (tower.tier - 1) * 0.12;
    }
  }
}

export function towerSellRefund(tower) {
  return Math.floor(tower.invested * SELL_RATIO);
}

export function createProjectile(src, from, targetEnemyRef, damage, speed, kind, splashRadius) {
  return {
    x: from.x,
    y: from.y,
    targetRef: targetEnemyRef,
    damage,
    speed,
    kind,
    splashRadius: splashRadius || 0,
    dead: false,
    color: src.baseDef.color,
  };
}

function dist(ax, ay, bx, by) {
  const dx = ax - bx;
  const dy = ay - by;
  return Math.hypot(dx, dy);
}

export function updateProjectile(game, proj, dt, enemies) {
  const tgt = proj.targetRef && proj.targetRef.current;
  let tx = proj.x;
  let ty = proj.y;
  if (tgt && tgt.hp > 0) {
    const p = game.gridToPixel(tgt.gx, tgt.gy);
    tx = p.x;
    ty = p.y;
  }
  const step = proj.speed * dt;
  const d = dist(proj.x, proj.y, tx, ty);
  if (d < step + 0.08 || (tgt && tgt.hp <= 0)) {
    proj.dead = true;
    const hitX = tgt && tgt.hp > 0 ? tx : proj.x;
    const hitY = tgt && tgt.hp > 0 ? ty : proj.y;
    if (proj.kind === "splash") {
      const R = proj.splashRadius * game.tileSize;
      for (const e of enemies) {
        if (e.hp <= 0) continue;
        const ep = game.gridToPixel(e.gx, e.gy);
        if (dist(hitX, hitY, ep.x, ep.y) <= R) {
          e.hp -= proj.damage;
        }
      }
    } else if (tgt && tgt.hp > 0) {
      tgt.hp -= proj.damage;
    }
    return;
  }
  proj.x += ((tx - proj.x) / d) * step;
  proj.y += ((ty - proj.y) / d) * step;
}

export function computeSupportBuffs(game) {
  const towers = game.towers;
  for (const tw of towers) {
    tw._buffD = 1;
    tw._buffR = 1;
  }
  const sups = towers.filter((t) => t.baseDef.kind === "support");
  for (const tw of towers) {
    if (tw.baseDef.kind === "support") continue;
    let bd = 1;
    let br = 1;
    const c = game.gridToPixel(tw.gx, tw.gy);
    for (const s of sups) {
      const cs = game.gridToPixel(s.gx, s.gy);
      if (dist(c.x, c.y, cs.x, cs.y) <= s.range * game.tileSize) {
        bd *= s.buffDamage;
        br *= s.buffSpeed;
      }
    }
    tw._buffD = bd;
    tw._buffR = br;
  }
}
