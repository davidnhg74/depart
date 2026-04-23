/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // standalone emits a minimal runtime at .next/standalone/ that
  // copies only the deps + pages needed. Dockerfile.fly builds with
  // this; local `npm run dev` is unaffected.
  output: 'standalone',
}

module.exports = nextConfig
