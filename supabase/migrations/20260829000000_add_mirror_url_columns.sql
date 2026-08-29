-- Add mirror URL columns to store all host URLs for redundancy.
-- When thumbnails are uploaded to multiple hosts (Catbox, Pixhost, freeimage.host),
-- all URLs are preserved so if one host goes down, the others still serve the image.
--
-- These are JSONB maps keyed by host name:
--   {"Catbox": "https://files.catbox.moe/abc.jpg", "Pixhost": "https://img2.pixhost.to/...", "freeimage.host": "https://i.ibb.co/..."}

-- recordings table
ALTER TABLE public.recordings
  ADD COLUMN IF NOT EXISTS thumbnail_mirrors JSONB,
  ADD COLUMN IF NOT EXISTS sprite_mirrors JSONB,
  ADD COLUMN IF NOT EXISTS preview_mirrors JSONB;

-- preview_images table
ALTER TABLE public.preview_images
  ADD COLUMN IF NOT EXISTS thumbnail_mirrors JSONB,
  ADD COLUMN IF NOT EXISTS sprite_mirrors JSONB,
  ADD COLUMN IF NOT EXISTS preview_mirrors JSONB;

-- pipeline_states table (for crash recovery)
ALTER TABLE public.pipeline_states
  ADD COLUMN IF NOT EXISTS thumb_mirrors JSONB,
  ADD COLUMN IF NOT EXISTS sprite_mirrors JSONB,
  ADD COLUMN IF NOT EXISTS preview_mirrors JSONB;
