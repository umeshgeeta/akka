package com.neosemantix.ds.paxos;

import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

/**
 * Main driver class which contains main to start the simulation.
 * 
 * Typically for Paxos implementation, there are 3 roles / types of actors:
 * - Proposer (the one who proposes a new proposal, subsequently with a value)
 * - Acceptor (the one who accesses proposals and accepts one or more)
 * - Learner (the who does not essentially participates in the process, but
 * 			learns about the final value which majority of Acceptors accept.)
 * 
 * In the current implementation, we are not going with any fixed subset of
 * distinguished Proposer or Learners. All are Proposers and Acceptors.
 * In other words, each Participant can propose and we need half of the 
 * participants - acceptors - to accept at least one proposal.
 * 
 * @author umeshpatil
 *
 */
public class PaxosMain {
	
	
	private static List<ActorRef> participants;
	
	
	private static Object gc;
	

	/**
	 * 
	 * @param args Right now command line arguments are ignored since inputs
	 * 				are sourced from Config class.
	 */
	public static void main(String[] args) {
		gc = new Object();
		Config cfg = Config.getInstance();
		final ActorSystem system = ActorSystem.create("Paxos");
		participants = new ArrayList<ActorRef>();
		for (int i=0; i< cfg.numParticipants; i++) {
			participants.add(system.actorOf(Participant.props(cfg.numParticipants, "Participant_" + i)));
		}
	}
	
	public static Object getGlocalCommon() {
		return gc;
	}
	
	public static List<ActorRef> getParticipants() {
		return participants;
	}
	
	public static void consensusReached() {
		System.exit(0);
	}

}
