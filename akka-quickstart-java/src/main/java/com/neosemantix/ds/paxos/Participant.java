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

	private static int NO_PROPOSAL_ACCEPTED_YET = -1;
	private static int PROPOSAL_VALUE_NOT_APPLICABLE = -1;

	private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	static public Props props(int pCount, String name) {
		return Props.create(Participant.class, () -> new Participant(pCount, name));
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
		private long birthdate;

		private CirculatedProposal(ProposalNumberGenerator propNumGen, int pc, Participant p, long bd) {
			propNumGenarator = propNumGen;
			participantCount = pc;
			parent = p;
			birthdate = bd;
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
			switch (participantCount) {
			case 0:
			case 1:
				result = true;
				break;
			case 2:
				if (arg == 1)
					result = true;
				break;
			default:
				if (arg >= (participantCount / 2)) {
					result = true;
				}
			}
			return result;
		}

		private void circulateAcceptProposal(Participant issuer) {
			if (valueOfHighestProposalReceived == 0) {
				valueOfHighestProposalReceived  = 5;
			}
			Protocol.AcceptRequest acptReq = new Protocol.AcceptRequest(issuedPrepareRequestNumber,
					valueOfHighestProposalReceived);
			acceptResponsesReceived = 0;
			// push it to all participants
			issuer.issueRequests(acptReq);
		}

		private synchronized void trackAcceptResponse() {
			acceptResponsesReceived++;
			// when acceptResponsesReceived == number of participants; consensus is reached
			// participantCount - 1 because we skip message to self
			if (acceptResponsesReceived == (participantCount - 1)) {
				System.out.println("==========================================================");
				System.out.println("Consensus reached in "+ (System.currentTimeMillis() - birthdate)  + " milliseonds for proposal from " + parent + "  highestProposalNoOfResponsesReceived: "  + 
							highestProposalNoOfResponsesReceived + " valueOfHighestProposalReceived: " + valueOfHighestProposalReceived);
				System.out.println("==========================================================");
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
			acceptedProposalNumber = NO_PROPOSAL_ACCEPTED_YET;
			acceptedProposalValue = PROPOSAL_VALUE_NOT_APPLICABLE;
			propNumOfLastPrepReqResd = NO_PROPOSAL_ACCEPTED_YET;
		}

	}

	private String name;
	private CirculatedProposal propCirculated;
	private RespondedProposal propResponded;
	private Random random;

	private static Object TICK_KEY = "TickKey";

	private static final class FirstTick {
	}

	private static final class Tick {
	}

	/**
	 * @param pc
	 *            Participant count
	 */
	public Participant(int pc, String n) {
		propResponded = new RespondedProposal();
		propCirculated = new CirculatedProposal(new ProposalNumberGenerator(propResponded), pc, this, System.currentTimeMillis());
		random = new Random();
		getTimers().startSingleTimer(TICK_KEY, new FirstTick(), Duration.ofMillis(((1+random.nextInt(9)) * 100)));
		this.name = n;
		log.info("Created " +  this + " with ActorRef as " + this.getSelf());
	}
	
	public String toString() {
		return name;
	}
	
	private long prepReqResponseDelay() {
		int i = random.nextInt(11);
		if (i == 0) {
			i++;
		}
		return i * 100;
	}

	private Protocol.PrepareResponse respond(Protocol.PrepareRequest prepReq) {
		Protocol.PrepareResponse response = null;
		if (propResponded.propNumOfLastPrepReqResd < prepReq.proposalNumber) {
			response = new Protocol.PrepareResponse(propResponded.acceptedProposalNumber,
					propResponded.acceptedProposalValue);
			propResponded.propNumOfLastPrepReqResd = prepReq.proposalNumber;
		}
		// else we ignore
		
		// artificial delay
		Object globalC = PaxosMain.getGlocalCommon();
		synchronized (globalC) {
			try {
				globalC.wait(prepReqResponseDelay());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
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
			log.info("Received "+ prepReq + " by " + this + " from " + getSender());
			Protocol.PrepareResponse resp = respond(prepReq);
			if (resp != null) {
				getSender().tell(resp, getSelf());
			}
			// else we do nothing
		}).match(Protocol.AcceptRequest.class, accpReq -> {
			log.info("Received " + accpReq + " by " + this + " from " + getSender());
			Protocol.AcceptResponse resp = respond(accpReq, getSender());
			if (resp != null) {
				getSender().tell(resp, getSelf());
			}
			// else we do nothing
		}).match(Protocol.PrepareResponse.class, prepResp -> {
			log.info(">->->- Received " + prepResp + " by " + this + " from " + getSender());
			propCirculated.trackPrepareResponse(prepResp.lastAcceptedProposal, prepResp.lastAcceptedProposalValue);
		}).match(Protocol.AcceptResponse.class, accpResp -> {
			log.info(">>->>- Received " + accpResp + " by "  + this + " from " + getSender());
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
