"""Reconciles the two branches manager fans a frame out to:

    manager --new_frame--> postprocessA --metadata_a--> manager   (metadata branch)
    manager --new_frame--> postprocessB --out_c-->      manager   (image chain, via C)

A frame_id is only complete once BOTH its image and its metadata have
arrived, since arrival order between the branches isn't guaranteed.
"""
from .config import LOG_EVERY_N_FRAMES, MAX_PENDING_BACKLOG


class FrameSync:

    def __init__(self, logger):
        self._logger = logger
        # Per frame_id: holds whichever of {'image', 'meta'} has arrived so
        # far.
        self.pending = {}
        self.sent_at = {}  # frame_id -> Time, for garbage-collecting stale pending entries
        self.sent_frame_ids = set()
        self.image_results = {}
        self.metadata_results = {}
        # resolved (completed or given-up-on) among sent frames
        self.emitted_count = 0
        self.total_sent = 0
        self.dropped_count = 0  # frames skipped at the source due to backlog

    def backlog_full(self):
        return len(self.pending) >= MAX_PENDING_BACKLOG

    def record_sent(self, frame_id, now):
        self.sent_at[frame_id] = now
        self.sent_frame_ids.add(frame_id)
        self.total_sent += 1

    def record_dropped(self):
        self.dropped_count += 1

    def has_image(self, frame_id):
        return 'image' in self.pending.get(frame_id, {})

    def has_meta(self, frame_id):
        return 'meta' in self.pending.get(frame_id, {})

    def on_image(self, frame_id, image, stats):
        """Record the image half for frame_id. Returns (image, meta) once
        both halves are in, else None."""
        entry = self.pending.setdefault(frame_id, {})
        if 'image' in entry:
            return None  # already handled (or unexpected re-delivery); ignore
        entry['image'] = image
        entry['image_stats'] = stats
        return self._pop_if_ready(frame_id)

    def on_metadata(self, frame_id, meta):
        """Record the metadata half for frame_id. Returns (image, meta) once
        both halves are in, else None."""
        entry = self.pending.setdefault(frame_id, {})
        if 'meta' in entry:
            return None  # already handled (or unexpected re-delivery); ignore
        entry['meta'] = meta
        return self._pop_if_ready(frame_id)

    def _pop_if_ready(self, frame_id):
        entry = self.pending.get(frame_id)
        if entry is None or 'image' not in entry or 'meta' not in entry:
            return None
        self.image_results[frame_id] = entry['image_stats']
        self.metadata_results[frame_id] = entry['meta']
        del self.pending[frame_id]
        self.sent_at.pop(frame_id, None)
        self.emitted_count += 1
        return entry['image'], entry['meta']

    def expire_stale(self, now, timeout_s):
        # Pure garbage collection: a frame that's lost for good (BEST_EFFORT
        # QoS permits this) would otherwise sit in `pending` forever,
        # permanently inflating the backlog count `backlog_full` uses.
        # Returns True if anything was expired, so the caller can re-check
        # whether the run just became finished.
        expired_any = False
        for frame_id, sent_time in list(self.sent_at.items()):
            if frame_id in self.pending and (now - sent_time).nanoseconds / 1e9 > timeout_s:
                if frame_id % LOG_EVERY_N_FRAMES == 0:
                    self._logger.info(
                        f'frame_id={frame_id} never completed (lost in transit under '
                        f'best-effort QoS); dropping it from the backlog count')
                del self.pending[frame_id]
                del self.sent_at[frame_id]
                self.emitted_count += 1
                expired_any = True
        return expired_any

    def is_finished(self, total_frames):
        return total_frames is not None and self.emitted_count >= total_frames

    def missing_frame_ids(self):
        return sorted(self.sent_frame_ids - set(self.image_results))
