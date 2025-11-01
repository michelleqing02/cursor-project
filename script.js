const form = document.querySelector('#name-form');
const input = document.querySelector('#name-input');
const shuffleButton = document.querySelector('#shuffle-button');
const downloadButton = document.querySelector('#download-button');
const canvas = document.querySelector('#art-canvas');
const ctx = canvas.getContext('2d');

const fontFamilies = [
  '"Monoton", cursive',
  '"Bungee", cursive',
  '"Concert One", sans-serif',
  '"Orbitron", sans-serif',
  '"Press Start 2P", cursive'
];

const palettes = [
  {
    name: 'Aurora',
    background: ['#061728', '#0d2a3d', '#143f5d'],
    text: ['#00f5d4', '#00bbf9', '#fe53bb'],
    sparks: ['#ffbf69', '#f5f5f5']
  },
  {
    name: 'Neon Sunset',
    background: ['#1a0b2e', '#42047e', '#07f49e'],
    text: ['#ff61d2', '#fe9090', '#fecf33'],
    sparks: ['#ffffff', '#ffd6ff']
  },
  {
    name: 'Solar Flare',
    background: ['#160f30', '#472783', '#f88c24'],
    text: ['#ffe066', '#ff5f6d', '#ffc371'],
    sparks: ['#fff9c4', '#ffadad']
  },
  {
    name: 'Cyber Dreams',
    background: ['#050505', '#09203f', '#1e3c72'],
    text: ['#a8ff78', '#78ffd6', '#00d4ff'],
    sparks: ['#ffffff', '#b3f8ff']
  },
  {
    name: 'Candy Pulse',
    background: ['#320d3e', '#641b4e', '#f55951'],
    text: ['#ff85a1', '#ffd4d4', '#ffe066'],
    sparks: ['#faf3dd', '#fcbf49']
  }
];

let lastName = 'Ada Lovelace';
let lastSeed = Math.random();

const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

function rngFromSeed(seed) {
  let t = Math.floor(seed * 2147483647) || 1;
  return function () {
    t ^= t << 13;
    t ^= t >>> 17;
    t ^= t << 5;
    return ((t < 0 ? ~t + 1 : t) % 2147483647) / 2147483647;
  };
}

function pick(array, rng) {
  return array[Math.floor(rng() * array.length)];
}

function fitCanvas(baseWidth, baseHeight) {
  const ratio = window.devicePixelRatio || 1;
  const width = Math.round(baseWidth);
  const height = Math.round(baseHeight);

  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(ratio, ratio);
}

function paintBackground(rng, palette, width, height) {
  const gradient = ctx.createLinearGradient(0, 0, width, height);
  palette.background.forEach((color, idx) => {
    gradient.addColorStop(idx / (palette.background.length - 1 || 1), color);
  });

  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, width, height);

  const layers = 4 + Math.floor(rng() * 3);
  for (let i = 0; i < layers; i += 1) {
    const hueShift = 20 + rng() * 80;
    const alpha = 0.08 + rng() * 0.12;
    const radius = (Math.min(width, height) / 2) * (0.4 + rng() * 0.8);
    const x = width * (0.1 + rng() * 0.8);
    const y = height * (0.1 + rng() * 0.8);

    const circleGradient = ctx.createRadialGradient(
      x,
      y,
      radius * 0.15,
      x,
      y,
      radius
    );

    circleGradient.addColorStop(0, `hsla(${hueShift}, 90%, 70%, ${alpha})`);
    circleGradient.addColorStop(1, 'rgba(0, 0, 0, 0)');

    ctx.save();
    ctx.globalCompositeOperation = 'lighter';
    ctx.fillStyle = circleGradient;
    ctx.beginPath();
    ctx.arc(x, y, radius, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }
}

function renderName(name, rng, palette, width, height) {
  const trimmed = name.trim();
  const fontFamily = pick(fontFamilies, rng);
  const baseSize = clamp(width / (trimmed.length * 0.7), 92, 210);
  const rotation = (rng() - 0.5) * 0.05; // subtle tilt

  const textGradient = ctx.createLinearGradient(
    width * 0.2,
    height * 0.3,
    width * 0.8,
    height * 0.7
  );
  palette.text.forEach((color, index) => {
    textGradient.addColorStop(index / (palette.text.length - 1 || 1), color);
  });

  ctx.save();
  ctx.translate(width / 2, height / 2);
  ctx.rotate(rotation);
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.lineJoin = 'round';

  ctx.shadowColor = 'rgba(255, 255, 255, 0.45)';
  ctx.shadowBlur = 35 + rng() * 25;
  ctx.shadowOffsetY = 6;

  ctx.font = `${baseSize}px ${fontFamily}`;
  ctx.fillStyle = textGradient;
  ctx.fillText(trimmed, 0, 0);

  ctx.shadowBlur = 0;
  ctx.lineWidth = clamp(baseSize * 0.04, 3, 12);
  ctx.strokeStyle = 'rgba(0, 0, 0, 0.38)';
  ctx.strokeText(trimmed, 0, 0);

  // Emboss highlight
  ctx.globalCompositeOperation = 'screen';
  ctx.lineWidth = clamp(baseSize * 0.015, 1.5, 6);
  ctx.strokeStyle = 'rgba(255, 255, 255, 0.35)';
  ctx.strokeText(trimmed, -baseSize * 0.01, -baseSize * 0.01);

  ctx.restore();
}

function sprinkleSparks(rng, palette, width, height) {
  const count = 64 + Math.floor(rng() * 86);
  ctx.save();
  ctx.globalCompositeOperation = 'lighter';

  for (let i = 0; i < count; i += 1) {
    const x = rng() * width;
    const y = rng() * height;
    const size = 0.5 + rng() * 2.8;
    const color = pick(palette.sparks, rng);
    ctx.fillStyle = color;
    ctx.globalAlpha = 0.4 + rng() * 0.6;
    ctx.beginPath();
    ctx.arc(x, y, size, 0, Math.PI * 2);
    ctx.fill();
  }

  ctx.restore();
}

function drawScanlines(width, height) {
  const lineHeight = 3;
  const alpha = 0.04;

  ctx.save();
  ctx.globalCompositeOperation = 'soft-light';
  ctx.fillStyle = `rgba(255, 255, 255, ${alpha})`;
  for (let y = 0; y < height; y += lineHeight) {
    ctx.fillRect(0, y, width, 1);
  }
  ctx.restore();
}

function forgeNameArt(name, seed = Math.random()) {
  if (!name.trim()) {
    name = 'Nameless Wonder';
  }

  lastName = name;
  lastSeed = seed;

  const rng = rngFromSeed(seed);
  const palette = pick(palettes, rng);

  const baseWidth = clamp(name.length * 120, 720, 1180);
  const baseHeight = clamp(baseWidth * 0.48, 320, 520);

  fitCanvas(baseWidth, baseHeight);

  paintBackground(rng, palette, baseWidth, baseHeight);
  renderName(name, rng, palette, baseWidth, baseHeight);
  sprinkleSparks(rng, palette, baseWidth, baseHeight);
  drawScanlines(baseWidth, baseHeight);
}

function handleForge(event) {
  if (event) {
    event.preventDefault();
  }
  forgeNameArt(input.value, Math.random());
}

function handleShuffle() {
  forgeNameArt(lastName, Math.random());
}

function handleDownload() {
  const link = document.createElement('a');
  link.href = canvas.toDataURL('image/png');
  const safeName = lastName.trim().replace(/[^a-z0-9]+/gi, '-').replace(/-+/g, '-');
  link.download = `${safeName || 'name-art'}.png`;
  link.click();
}

let resizeTimeout;
function handleResize() {
  clearTimeout(resizeTimeout);
  resizeTimeout = setTimeout(() => {
    forgeNameArt(lastName, lastSeed);
  }, 180);
}

form.addEventListener('submit', handleForge);
shuffleButton.addEventListener('click', handleShuffle);
downloadButton.addEventListener('click', handleDownload);
window.addEventListener('resize', handleResize);

input.addEventListener('focus', () => input.select());

// Initialize with default
forgeNameArt(lastName, lastSeed);
