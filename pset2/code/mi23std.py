#!/usr/bin/python

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer


class Mi23Std(Peer):

    def post_init(self):
        print "post_init(): %s here!" % self.id
        self.piece_ownership = dict()
        self.num_open_slots = 4
        self.lucky_peer = None
        self.lucky_counter = 0

    def update_piece_ownership(self, peers):
        """
        Create dictionary of needed pieces and the agents who have them
        """
        for peer in peers:
            for piece in peer.available_pieces:
                if piece not in self.piece_ownership:
                    self.piece_ownership[piece] = set([peer])
                else:
                    self.piece_ownership[piece].add(peer)

    def order_rarest_pieces(self, needed_pieces):
        np = list(needed_pieces)
        np.sort(key=lambda n: len(self.piece_ownership[n]))
        return np

    def get_download_history(self, history):
        """
        Create dictionary of how much the client downloaded from its peers
        in the last two rounds
        """
        dl_hist_dict = dict()

        last_round = history.last_round()

        # first look at dl history from last round
        for dl_hist_past in history.downloads[last_round]:
            dl_hist_dict[dl_hist_past.from_id] = dl_hist_past.blocks

        # if we are in the 1st round, there is no data from 2 rounds ago
        if last_round == 0:
            return dl_hist_dict

        # then, look at dl history from current round
        for dl_hist_curr in history.downloads[last_round-1]:
            if dl_hist_curr.from_id not in dl_hist_dict:
                dl_hist_dict[dl_hist_curr.from_id] = dl_hist_curr.blocks
            else:
                dl_hist_dict[dl_hist_curr.from_id] += dl_hist_curr.blocks

        return dl_hist_dict

    def order_download_rate(self, history, requests):
        """
        Create dictionary of download rate, rank the requesters in the order
        of download rate
        """

        download_hist_dict = self.get_download_history(history)

        r_ids = [request.requester_id for request in requests]
        r_ids.sort(key=
            lambda r:
            download_hist_dict[r] if r in download_hist_dict
            else 0
        )
        return r_ids

    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see

        returns: a list of Request() objects

        This will be called after update_pieces() with the most recent state.
        """
        needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = filter(needed, range(len(self.pieces)))
        np_set = set(needed_pieces)  # sets support fast intersection ops.

        logging.debug("%s here: still need pieces %s" % (
            self.id, needed_pieces))

        # logging.debug("%s still here. Here are some peers:" % self.id)
        # for p in peers:
        #     logging.debug(
        #         "id: %s, available pieces: %s" % (p.id, p.available_pieces)
        #     )

        # logging.debug("And look, I have my entire history available too:")
        # logging.debug(str(history))

        requests = []   # We'll put all the things we want here
        # Symmetry breaking is good...
        random.shuffle(needed_pieces)

        # Update ownerships of neighbors!
        self.update_piece_ownership(peers)

        # request all available pieces from all peers!
        # (up to self.max_requests from each)
        for peer in peers:
            av_set = set(peer.available_pieces)
            isect = av_set.intersection(np_set)
            # order the pieces we can get in rarest-first order
            prioritized_pieces = self.order_rarest_pieces(list(isect))

            n = min(self.max_requests, len(isect))

            # request the first `n' rarest pieces that the peer has
            for piece_id in prioritized_pieces[:n]:
                # aha! The peer has this piece! Request it.
                # which part of the piece do we need next?
                # (must get the next-needed blocks in order)
                start_block = self.pieces[piece_id]
                r = Request(self.id, peer.id, piece_id, start_block)
                requests.append(r)

        return requests

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """

        round = history.current_round()
        logging.debug("%s again.  It's round %d." % (
            self.id, round))
        # One could look at other stuff in the history too here.
        # For example, history.downloads[round-1] (if round != 0, of course)
        # has a list of Download objects for each Download to this peer in
        # the previous round.

        if len(requests) == 0:

            msg = "Unwanted pieces: "

            have_a_full_piece = False
            
            for num_pieces in self.pieces:
                if num_pieces == self.conf.blocks_per_piece:
                    have_a_full_piece = True

                msg += str(num_pieces)
                msg += ","

            if not have_a_full_piece:
                msg = "No full piece"

            logging.debug(msg)

            chosen = []
            bws = []

        else:
            logging.debug(
                "\n Still here: upload to peers with highest download rate \n"
            )

            ordered_request_ids = self.order_download_rate(history, requests)

            chosen = ordered_request_ids[:self.num_open_slots - 1]

            # optimistically choose to unchoke one random request every 30 sec

            have_lucky_to_replace = (
                len(ordered_request_ids) >= self.num_open_slots
            )

            time_to_change_lucky = (
                self.lucky_peer is None or self.lucky_counter % 3 == 0
            )

            if have_lucky_to_replace and time_to_change_lucky:

                optimistic_unchoked_request_id = random.choice(
                    ordered_request_ids[self.num_open_slots - 1:]
                )

                self.lucky_peer = optimistic_unchoked_request_id
                self.lucky_counter = 0

            if self.lucky_peer is not None:
                chosen.append(self.lucky_peer)
                self.lucky_counter += 1

            chosen_str = "Our chosen: " + str(chosen)
            requests_str = "Our requests: " + str(ordered_request_ids)
            logging.debug(
                chosen_str
            )

            logging.debug(
                requests_str
            )

            logging.debug(
                "\nLucky Peer for %s: %s.\n" % (self.id, self.lucky_peer)
            )

            # Evenly "split" my upload bandwidth among the one chosen requester
            bws = even_split(self.up_bw, len(chosen))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]

        return uploads
