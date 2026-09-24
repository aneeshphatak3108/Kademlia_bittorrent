import random

from bittorrent import constants
from bittorrent.metadata import TorrentMetadata, build_torrent
from bittorrent.picker import PiecePicker

BLOCK = constants.BLOCK_SIZE

# Comfortably more blocks than ENDGAME_THRESHOLD, so tests exercise normal
# (non-endgame) behaviour unless they deliberately shrink the torrent.
MANY = constants.ENDGAME_THRESHOLD + 12


def metadata(pieces: int = MANY, piece_length: int = BLOCK):
    """A torrent of `pieces` pieces, one block each by default, for easy arithmetic."""
    return TorrentMetadata.from_bytes(
        build_torrent(b"a" * (pieces * piece_length), "t.bin", piece_length)
    )


def picker_past_random_first(pieces: int = MANY, piece_length: int = BLOCK):
    """A picker holding enough pieces to be past the random-first-piece phase."""
    meta = metadata(pieces, piece_length)
    have = set(range(constants.RANDOM_FIRST_PIECES))
    return PiecePicker(meta, have=have), meta, have


def test_picks_the_rarest_piece_first():
    picker, meta, have = picker_past_random_first()
    wanted = set(range(meta.piece_count)) - have
    rare = max(wanted)
    for index in wanted:
        picker.availability[index] = 1 if index == rare else 5

    blocks = picker.pick(wanted, "peer-a", count=1)
    assert [b[0] for b in blocks] == [rare]


def test_rarest_first_ordering_across_several_picks():
    picker, meta, have = picker_past_random_first()
    a, b, c = sorted(set(range(meta.piece_count)) - have)[:3]
    for index in range(meta.piece_count):
        picker.availability[index] = 99
    picker.availability[a] = 3
    picker.availability[b] = 1
    picker.availability[c] = 2

    blocks = picker.pick({a, b, c}, "peer-a", count=3)
    assert [blk[0] for blk in blocks] == [b, c, a]


def test_random_first_pieces_ignores_rarity():
    meta = metadata(pieces=6)
    picker = PiecePicker(meta, have=set())  # nothing yet -> random-first phase
    picker.availability = [1, 9, 9, 9, 9, 9]  # piece 0 is by far the rarest
    random.seed(1234)
    firsts = {picker.pick({0, 1, 2, 3, 4, 5}, f"p{i}", count=1)[0][0] for i in range(20)}
    # Under rarest-first this would always be piece 0.
    assert firsts != {0}


def test_ties_are_broken_randomly():
    picker, meta, have = picker_past_random_first()
    wanted = set(range(meta.piece_count)) - have
    for index in wanted:
        picker.availability[index] = 5  # all equally rare

    random.seed(7)
    picked = set()
    for i in range(25):
        peer = f"peer{i}"
        blocks = picker.pick(wanted, peer, count=1)
        assert blocks, "ran out of pickable blocks"
        picked.add(blocks[0][0])
        picker.release_peer(peer)  # unclaim, so every iteration sees a full pool
    assert len(picked) > 1, "all ties resolved to the same piece -- not randomized"


def test_block_is_claimed_so_a_second_peer_does_not_get_it():
    picker, meta, have = picker_past_random_first()
    wanted = set(range(meta.piece_count)) - have
    first = picker.pick(wanted, "peer-a", count=3)
    second = picker.pick(wanted, "peer-b", count=3)
    assert first, "first peer should get blocks"
    assert not set(second) & set(first), "the same block was handed to two peers"


def test_multi_block_piece_is_split_into_blocks():
    picker, _, _ = picker_past_random_first(pieces=MANY, piece_length=BLOCK * 3)
    blocks = picker.pick({5}, "peer-a", count=10)
    assert [(b[1], b[2]) for b in blocks] == [(0, BLOCK), (BLOCK, BLOCK), (2 * BLOCK, BLOCK)]


def test_short_final_block_has_the_remainder_length():
    # Last piece of this torrent is BLOCK + 100 bytes... make the whole torrent
    # one piece + remainder so the final piece is short.
    meta = TorrentMetadata.from_bytes(build_torrent(b"z" * (BLOCK + 100), "t.bin", BLOCK * 2))
    picker = PiecePicker(meta, have=set())
    blocks = picker.pick({0}, "peer-a", count=10)
    assert [(b[1], b[2]) for b in blocks] == [(0, BLOCK), (BLOCK, 100)]


def test_never_picks_a_piece_we_already_have():
    meta = metadata(pieces=3)
    picker = PiecePicker(meta, have={0, 1, 2})
    assert picker.pick({0, 1, 2}, "peer-a", count=5) == []


def test_only_picks_pieces_the_peer_actually_has():
    picker, _, _ = picker_past_random_first()
    blocks = picker.pick({7}, "peer-a", count=5)
    assert {b[0] for b in blocks} == {7}


def test_block_received_reports_piece_completion():
    meta = metadata(pieces=1, piece_length=BLOCK * 2)
    picker = PiecePicker(meta, have=set())
    assert picker.block_received(0, 0) is False
    assert picker.block_received(0, BLOCK) is True


def test_received_blocks_are_not_re_picked():
    picker, _, _ = picker_past_random_first(pieces=MANY, piece_length=BLOCK * 2)
    picker.block_received(5, 0)
    blocks = picker.pick({5}, "peer-b", count=5)
    assert [b[1] for b in blocks] == [BLOCK]


def test_release_peer_makes_its_blocks_pickable_again():
    picker, meta, have = picker_past_random_first()
    wanted = set(range(meta.piece_count)) - have
    first = picker.pick(wanted, "peer-a", count=3)
    assert first
    before_release = picker.pick(wanted, "peer-b", count=3)
    assert not set(before_release) & set(first), "claimed blocks leaked to another peer"

    picker.release_peer("peer-a")  # peer-a disconnects
    after_release = picker.pick(wanted, "peer-c", count=3)
    assert set(first) <= set(after_release) or set(after_release) & set(first), (
        "released blocks should become pickable again"
    )


def test_piece_failed_discards_partial_progress():
    picker, _, _ = picker_past_random_first(pieces=MANY, piece_length=BLOCK * 2)
    picker.block_received(5, 0)
    picker.piece_failed(5)  # hash check failed -- whole piece re-requested
    blocks = picker.pick({5}, "peer-a", count=5)
    assert [b[1] for b in blocks] == [0, BLOCK]


def test_piece_verified_marks_it_have_and_clears_state():
    picker, _, _ = picker_past_random_first()
    picker.pick({5}, "peer-a", count=1)
    picker.block_received(5, 0)
    picker.piece_verified(5)
    assert 5 in picker.have
    assert picker.pick({5}, "peer-b", count=1) == []


def test_endgame_allows_duplicate_requests_to_different_peers():
    # One block left overall -> endgame, so a second peer may also be asked.
    meta = metadata(pieces=1)
    picker = PiecePicker(meta, have=set())
    first = picker.pick({0}, "peer-a", count=1)
    second = picker.pick({0}, "peer-b", count=1)
    assert first and second and first == second


def test_endgame_does_not_duplicate_to_the_same_peer():
    meta = metadata(pieces=1)
    picker = PiecePicker(meta, have=set())
    picker.pick({0}, "peer-a", count=1)
    assert picker.pick({0}, "peer-a", count=1) == []


def test_no_endgame_duplicates_while_plenty_remains():
    picker, meta, have = picker_past_random_first()
    wanted = set(range(meta.piece_count)) - have
    first = picker.pick(wanted, "peer-a", count=len(wanted))
    second = picker.pick(wanted, "peer-b", count=len(wanted))
    assert first and not second, "duplicated requests outside endgame"


def test_availability_tracks_peers_joining_and_leaving():
    meta = metadata(pieces=3)
    picker = PiecePicker(meta)
    picker.add_peer({0, 1})
    picker.add_peer({1})
    assert picker.availability == [1, 2, 0]
    picker.peer_got_piece(2)
    assert picker.availability == [1, 2, 1]
    picker.remove_peer({0, 1})
    assert picker.availability == [0, 1, 1]


def test_availability_never_goes_negative():
    meta = metadata(pieces=2)
    picker = PiecePicker(meta)
    picker.remove_peer({0, 1})
    assert picker.availability == [0, 0]


def test_is_complete_and_missing_pieces():
    meta = metadata(pieces=3)
    picker = PiecePicker(meta, have={0, 2})
    assert not picker.is_complete
    assert picker.missing_pieces() == {1}
    picker.piece_verified(1)
    assert picker.is_complete
