#!/usr/bin/python

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer


class Mi23Tyrant(Peer):

    def post_init(self):
        print "post_init(): %s here!" % self.id
        self.piece_ownership = dict()
        self.num_open_slots = 4
        self.gamma = 0.1
        self.r = 3
        self.alpha = 0.2
        self.estimated_dl_rate = dict()
        self.estimated_up_threshold = dict()
        self.cap = self.up_bw
        self.unchoked_hist = dict()
        self.round = 0

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

        # shuffle first in order to break symmetry if two piece equally rare
        random.shuffle(np)

        np.sort(key=lambda n: len(self.piece_ownership[n]))
        return np

    def get_download_history(self, peers, history):
        """
        Create dictionary of which peers uploaded to client in last round
        """
        dl_hist_dict = dict()

        last_round = history.last_round()

        if last_round == -1:
            return dl_hist_dict

        # look at the download history from last round to see who uploaded
        for dl_hist_past in history.downloads[last_round]:
            if dl_hist_past.from_id not in dl_hist_dict.keys():
                dl_hist_dict[dl_hist_past.from_id] = True
        # set everyone else to False
        for peer in peers:
            if peer.id not in dl_hist_dict.keys():
                dl_hist_dict[peer.id] = False

        return dl_hist_dict

    def order_ratio(self, requests):
        """
        Get the ordering of peers by the ratio of their dl/ul
        """
        r_ids = list(set([request.requester_id for request in requests]))
        up_thresholds = []

        ratio_dict = dict()
        for r_id in r_ids:
            ratio_dict[r_id] = (
                (float(self.estimated_dl_rate[r_id]) /
                 self.estimated_up_threshold[r_id])
            )

        r_ids.sort(key=lambda k: ratio_dict[k], reverse=True)
        up_thresholds = [self.estimated_up_threshold[r_id] for r_id in r_ids]

        final_ids = []
        final_up_bws = []

        bw_sum = 0
        for tau, r_id in zip(up_thresholds, r_ids):
            bw_sum += tau
            if bw_sum < self.cap:
                final_ids.append(r_id)
                final_up_bws.append(tau)
            else:
                break

        return final_ids, final_up_bws

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

        requests = []   # We'll put all the things we want here
        # Symmetry breaking is good...
        random.shuffle(needed_pieces)

        # Update ownerships of neighbors!
        self.update_piece_ownership(peers)

        dl_hist = self.get_download_history(peers, history)
        # request all available pieces from all peers!
        # (up to self.max_requests from each)
        for peer in peers:
            self.round += 1
            # check if peer had unchoked us last round
            if peer.id in dl_hist.keys():
                # keep track of how long we had been unchoked by the peer
                if peer.id not in self.unchoked_hist.keys():
                    # theoretically this shouldn't happen, but just in case
                    self.unchoked_hist[peer.id] = 1
                else:
                    self.unchoked_hist[peer.id] += 1
                # update the estimated download flow
                self.estimated_dl_rate[peer.id] = (
                    len(peer.available_pieces) / float(self.round)
                )
                # initialize upload threshold if have not done it already
                if peer.id not in self.estimated_up_threshold.keys():
                    self.estimated_up_threshold[peer.id] = self.up_bw / 3.0
                # if unchoked for multiple rounds, adjust threshold
                elif self.unchoked_hist[peer.id] == self.r:
                    curr_threshold = self.estimated_up_threshold[peer.id]
                    self.estimated_up_threshold[peer.id] = (
                        (1 - self.gamma) * curr_threshold
                    )
                    # reset round counter
                    self.unchoked_hist[peer.id] = 0
            # update threshold if peer did not unchoke us
            else:
                # don't initialize threshold if we haven't interacted yet
                if peer.id not in self.estimated_up_threshold.keys():
                    pass
                else:
                    self.estimated_up_threshold[peer.id] = (
                        (1 + self.alpha) * self.estimated_up_threshold[peer.id]
                    )
                # reset round counter
                self.unchoked_hist[peer.id] = 0

            av_set = set(peer.available_pieces)
            isect = av_set.intersection(np_set)
            # order the pieces we can get in rarest-first order
            prioritized_pieces = self.order_rarest_pieces(list(isect))

            # prioritize further the pieces that we already have blocks for
            # if random.random() > 0.5:
                # prioritized_pieces.sort(
                #     key=lambda k: self.pieces[k], reverse=True
                # )

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

            # debugging
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
                "My requests are: %s" % (
                    [
                        "Requester: %s, Requested piece: %s" %
                        (r.requester_id, r.piece_id)
                        for r in requests
                    ]
                )
            )
            logging.debug(
                "\n Still here: upload to peers with highest download rate \n"
            )

            # Evenly "split" my upload bandwidth among the one chosen requester
            chosen, bws = self.order_ratio(requests)

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]

        return uploads
