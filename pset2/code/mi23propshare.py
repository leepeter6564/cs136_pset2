#!/usr/bin/python

import random
import logging

from messages import Upload, Request
from util import even_split
from peer import Peer
from mi23std import Mi23Std


class Mi23PropShare(Mi23Std):

    def post_init(self):
        print "post_init(): %s here!" % self.id
        self.piece_ownership = dict()
        self.percent_opt_bw = 0.1

    def get_download_history(self, history):
        """
        Create dictionary of how much the client downloaded from its peers
        in the last round
        """
        dl_hist_dict = dict()

        last_round = history.last_round()

        # first look at dl history from last round
        for dl_hist_past in history.downloads[last_round]:
            if dl_hist_past.from_id not in dl_hist_dict.keys():
                dl_hist_dict[dl_hist_past.from_id] = dl_hist_past.blocks
            else:
                dl_hist_dict[dl_hist_past.from_id] += dl_hist_past.blocks

        logging.debug("%s past download history : %s" % (
            self.id, str(dl_hist_dict)
            )
        )

        return dl_hist_dict

    def get_dl_proportions(self, history, requests):
        """
        Create dictionary of download rate, rank the requesters in the order
        of download rate
        """

        download_hist_dict = self.get_download_history(history)

        valid_requesters = []
        amount_downloaded = []

        # remove duplicates, so that client can upload to more varied peers
        r_ids = list(set([request.requester_id for request in requests]))

        # check if client had downloaded from requester
        for r_id in r_ids:
            if r_ids in download_hist_dict.keys():
                valid_requesters.append(r_id)
                amount_downloaded.append(download_hist_dict[r_id])

        dl_prop = [
            a/float(sum(amount_downloaded))*(1-self.percent_opt_bw)*self.up_bw
            for a
            in amount_downloaded
        ]

        # get the optimistically unchoked candidate
        opt_chosen = random.choice(list(set(r_ids) - set(valid_requesters)))
        valid_requesters.append(opt_chosen)

        # if there were no requests and we are only optimistically unchocking,
        # don't limit bandwidth
        if len(valid_requesters) == 1:
            dl_prop.append(self.up_bw)
        # otherwise use the reserved amount of bandwidth
        else:
            dl_prop.append(self.percent_opt_bw*self.up_bw)

        return valid_requesters, dl_prop

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
        # random.shuffle(needed_pieces)

        # Update ownerships of neighbors!
        self.update_piece_ownership(peers)

        # request all available pieces from all peers!
        # (up to self.max_requests from each)
        for peer in peers:
            av_set = set(peer.available_pieces)
            isect = av_set.intersection(np_set)
            # order the pieces we can get in rarest-first order
            prioritized_pieces = self.order_rarest_pieces(list(isect))

            # randomize whether to prioritize rarest or blocks already had!
            if random.random() > 0.5:
                prioritized_pieces.sort(
                    key=lambda k: self.pieces[k], reverse=True
                )

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
                "\nStill here: \
                upload to peers with proportional download rate\n"
            )

            chosen, bws = self.get_dl_proportions(history, requests)

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]

        return uploads
