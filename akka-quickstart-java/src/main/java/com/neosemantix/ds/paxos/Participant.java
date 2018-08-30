/**
 * 
 */
package com.neosemantix.ds.paxos;

import java.time.Duration;
import java.util.List;
import java.util.Random;

//import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * @author umeshpatil
 *
 */
public class Participant extends AbstractActorWithTimers {
	// AbstractActor {

	private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	static public Props props(int pCount) {
		return Props.create(Participant.class, () -> new Participant(pCount));
	}

	private static class ProposalNumberGenerator {

		private RespondedProposal respondedProposals;

		private ProposalNumberGenerator(RespondedProposal respProp) {
			respondedProposals = respProp;
		}

		private int getNextProposalNumber() {
			int pn = Math.max(respondedProposals.propNumOfLastPrepReqResd, respondedProposals.acceptedProposalNumber);
			return pn + 1;
		}

	}

	private static class CirculatedProposal {

		private int participantCount;
		private int issuedPrepareRequestNumber;
		private ProposalNumberGenerator propNumGenarator;
		private int preparResponsesReceived;
		private int acceptResponsesReceived;
		private int highestProposalNoOfResponsesReceived;
		private int valueOfHighestProposalReceived;
		private Participant parent;

		private CirculatedProposal(ProposalNumberGenerator propNumGen, int pc, Participant p) {
			propNumGenarator = propNumGen;
			participantCount = pc;
			parent = p;
		}

		private int circulateNewPrepareProposal(Participant issuer) {
			// push this new proposal numbers to all participants
			preparResponsesReceived = 0;
			highestProposalNoOfResponsesReceived = 0;
			valueOfHighestProposalReceived = 0;
			issuedPrepareRequestNumber = propNumGenarator.getNextProposalNumber();
			Protocol.PrepareRequest prepReq = new Protocol.PrepareRequest(issuedPrepareRequestNumber);
			issuer.issueRequests(prepReq);
			return issuedPrepareRequestNumber;
		}

		private synchronized void trackPrepareResponse(int highestPropNo, int propVal) {
			preparResponsesReceived++;
			if (highestProposalNoOfResponsesReceived < highestPropNo) {
				highestProposalNoOfResponsesReceived = highestPropNo;
				valueOfHighestProposalReceived = propVal;
			}
			if (majority(preparResponsesReceived)) {
				// we got majorities responding to the prepare proposal
				// circulate the accept proposal now
				circulateAcceptProposal(parent);
			}
			// else will have to wait for more responses
		}

		private boolean majority(int arg) {
			boolean result = false;
			if (arg >= ((participantCount / 2) + 1)) {
				result = true;
			}
			return result;
		}

		private void circulateAcceptProposal(Participant issuer) {
			Protocol.AcceptRequest acptReq = new Protocol.AcceptRequest(issuedPrepareRequestNumber,
					valueOfHighestProposalReceived);
			acceptResponsesReceived = 0;
			// push it to all participants
			issuer.issueRequests(acptReq);
		}

		private synchronized void trackAcceptResponse() {
			acceptResponsesReceived++;
			// when acceptResponsesReceived == number of participants; consensus is reached
			if (acceptResponsesReceived == participantCount) {
				System.out.println("Consensus reached");
				PaxosMain.consensusReached();
			}
		}

	}

	private static class RespondedProposal {

		// Proposal number of last prepared request responded
		private int propNumOfLastPrepReqResd;

		// accepted proposals
		private int acceptedProposalNumber;
		private int acceptedProposalValue;
		// private ActorRef proposer;

		private RespondedProposal() {

		}

	}

	private CirculatedProposal propCirculated;
	private RespondedProposal propResponded;

	private static Object TICK_KEY = "TickKey";

	private static final class FirstTick {
	}

	private static final class Tick {
	}

	/**
	 * @param pc
	 *            Participant count
	 */
	public Participant(int pc) {
		propResponded = new RespondedProposal();
		propCirculated = new CirculatedProposal(new ProposalNumberGenerator(propResponded), pc, this);
		Random r = new Random();
		getTimers().startSingleTimer(TICK_KEY, new FirstTick(), Duration.ofMillis(((1+r.nextInt(9)) * 100)));
		log.info("Created " +  this);
	}

	private Protocol.PrepareResponse respond(Protocol.PrepareRequest prepReq) {
		Protocol.PrepareResponse response = null;
		if (propResponded.propNumOfLastPrepReqResd < prepReq.proposalNumber) {
			response = new Protocol.PrepareResponse(propResponded.acceptedProposalNumber,
					propResponded.acceptedProposalValue);
			propResponded.propNumOfLastPrepReqResd = prepReq.proposalNumber;
		}
		// else we ignore
		return response;
	}

	private Protocol.AcceptResponse respond(Protocol.AcceptRequest accpReq, ActorRef proposer) {
		Protocol.AcceptResponse resp = null;
		if (accpReq.proposalNumber >= propResponded.propNumOfLastPrepReqResd) {
			resp = new Protocol.AcceptResponse();
			propResponded.acceptedProposalNumber = accpReq.proposalNumber;
			propResponded.acceptedProposalValue = accpReq.proposalValue;
			// propResponded.proposer = proposer;
		}
		return resp;
	}

	private synchronized void issueRequests(Protocol.Request req) {
		List<ActorRef> participants = PaxosMain.getParticipants();
		if (participants != null && !participants.isEmpty()) {
			for (ActorRef p : participants) {
				if (p.compareTo(getSelf()) != 0) {
					p.tell(req, getSelf());
				}
				// else skip messages to self
			}
		}
	}

	@Override
	public Receive createReceive() {
		ActorRef ar = getSender();
		return receiveBuilder().match(Protocol.PrepareRequest.class, prepReq -> {
			log.info("Received prepare request "+ prepReq + " by " + this);
			Protocol.PrepareResponse resp = respond(prepReq);
			if (resp != null) {
				ActorRef arr = getSender();
				getSender().tell(resp, getSelf());
			}
			// else we do nothing
		}).match(Protocol.AcceptRequest.class, accpReq -> {
			log.info("Received accept request " + accpReq + " by " + this);
			Protocol.AcceptResponse resp = respond(accpReq, getSender());
			if (resp != null) {
				getSender().tell(resp, getSelf());
			}
			// else we do nothing
		}).match(Protocol.PrepareResponse.class, prepReq -> {
			log.info("Received prepare response by " + this);
			propCirculated.trackPrepareResponse(prepReq.lastAcceptedProposal, prepReq.lastAcceptedProposalValue);
		}).match(Protocol.AcceptResponse.class, accpReq -> {
			log.info("Received accept response by " + this);
			propCirculated.trackAcceptResponse();
		}).match(FirstTick.class, message -> {
			log.info("Circulating prepare request by " + this);
			// do something useful here
			propCirculated.circulateNewPrepareProposal(this);
			getTimers().startPeriodicTimer(TICK_KEY, new Tick(), Duration.ofSeconds(1));
		}).match(Tick.class, message -> {
			log.debug("Circulating prepare request by " + this);
			// do something useful here
			propCirculated.circulateNewPrepareProposal(this);
		}).build();
	}

}
