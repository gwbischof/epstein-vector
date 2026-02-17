"use client";

import { useEffect, useRef } from "react";

interface Star {
  x: number;
  y: number;
  z: number; // depth 0-1, affects size + brightness
  phase: number; // twinkle phase offset
  speed: number; // twinkle speed
}

interface Constellation {
  stars: number[]; // indices into star array
  alpha: number;
  drift: number;
}

const STAR_COUNT = 280;
const CONSTELLATION_COUNT = 6;
const CONSTELLATION_SIZE = [3, 5]; // min/max stars per constellation

export function Starfield() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animRef = useRef<number>(0);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let w = 0;
    let h = 0;

    const resize = () => {
      const dpr = window.devicePixelRatio || 1;
      w = window.innerWidth;
      h = window.innerHeight;
      canvas.width = w * dpr;
      canvas.height = h * dpr;
      canvas.style.width = `${w}px`;
      canvas.style.height = `${h}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };
    resize();
    window.addEventListener("resize", resize);

    // Generate stars
    const stars: Star[] = Array.from({ length: STAR_COUNT }, () => ({
      x: Math.random() * w,
      y: Math.random() * h,
      z: Math.random(),
      phase: Math.random() * Math.PI * 2,
      speed: 0.3 + Math.random() * 1.2,
    }));

    // Build constellation groups — nearby stars linked
    const constellations: Constellation[] = [];
    const used = new Set<number>();
    for (let c = 0; c < CONSTELLATION_COUNT; c++) {
      const size =
        CONSTELLATION_SIZE[0] +
        Math.floor(Math.random() * (CONSTELLATION_SIZE[1] - CONSTELLATION_SIZE[0] + 1));
      // pick a seed star
      let seed = Math.floor(Math.random() * STAR_COUNT);
      while (used.has(seed)) seed = (seed + 1) % STAR_COUNT;
      const group = [seed];
      used.add(seed);

      for (let i = 1; i < size; i++) {
        // find nearest unused star to the last star in group
        const last = stars[group[group.length - 1]];
        let bestIdx = -1;
        let bestDist = Infinity;
        for (let j = 0; j < STAR_COUNT; j++) {
          if (used.has(j)) continue;
          const dx = stars[j].x - last.x;
          const dy = stars[j].y - last.y;
          const dist = dx * dx + dy * dy;
          if (dist < bestDist && dist < (w * 0.18) ** 2) {
            bestDist = dist;
            bestIdx = j;
          }
        }
        if (bestIdx === -1) break;
        group.push(bestIdx);
        used.add(bestIdx);
      }

      if (group.length >= 2) {
        constellations.push({
          stars: group,
          alpha: 0.04 + Math.random() * 0.06,
          drift: (Math.random() - 0.5) * 0.02,
        });
      }
    }

    const draw = (t: number) => {
      ctx.clearRect(0, 0, w, h);

      // Subtle radial gradient background glow
      const grad = ctx.createRadialGradient(w * 0.5, h * 0.4, 0, w * 0.5, h * 0.4, w * 0.7);
      grad.addColorStop(0, "rgba(6, 182, 212, 0.012)");
      grad.addColorStop(0.5, "rgba(139, 92, 246, 0.006)");
      grad.addColorStop(1, "transparent");
      ctx.fillStyle = grad;
      ctx.fillRect(0, 0, w, h);

      // Draw constellation lines
      for (const c of constellations) {
        const flicker = Math.sin(t * 0.0003 + c.drift * 100) * 0.02;
        ctx.strokeStyle = `rgba(34, 211, 238, ${c.alpha + flicker})`;
        ctx.lineWidth = 0.5;
        ctx.beginPath();
        for (let i = 0; i < c.stars.length; i++) {
          const s = stars[c.stars[i]];
          if (i === 0) ctx.moveTo(s.x, s.y);
          else ctx.lineTo(s.x, s.y);
        }
        ctx.stroke();
      }

      // Draw stars
      for (const s of stars) {
        const twinkle = Math.sin(t * 0.001 * s.speed + s.phase);
        const brightness = 0.3 + s.z * 0.5 + twinkle * 0.2;
        const radius = 0.4 + s.z * 1.3;

        // Star core
        ctx.beginPath();
        ctx.arc(s.x, s.y, radius, 0, Math.PI * 2);
        ctx.fillStyle = `rgba(226, 232, 240, ${brightness})`;
        ctx.fill();

        // Larger glow for bright stars
        if (s.z > 0.6) {
          ctx.beginPath();
          ctx.arc(s.x, s.y, radius * 3, 0, Math.PI * 2);
          const glowColor =
            s.z > 0.85
              ? `rgba(34, 211, 238, ${brightness * 0.08})`
              : `rgba(167, 139, 250, ${brightness * 0.06})`;
          ctx.fillStyle = glowColor;
          ctx.fill();
        }
      }

      animRef.current = requestAnimationFrame(draw);
    };

    animRef.current = requestAnimationFrame(draw);

    return () => {
      cancelAnimationFrame(animRef.current);
      window.removeEventListener("resize", resize);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      className="fixed inset-0 z-0 pointer-events-none"
      aria-hidden
    />
  );
}
