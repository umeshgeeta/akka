package com.neosemantix.ds.paxos;

/**
 * Basic communication protocol followed by participants of this simulation.
 * It defines various request and response objects used by participants to
 * communicate with other participants. In other words, these are the objects
 * which are used by participants to send each other using the Akka framework.
 * 
 * @author umeshpatil
 */
public class Protocol {
	
	public static class Request {
		
		public final int proposalNumber;
		
		public Request(int pn) {
			proposalNumber = pn;
		}
		
		public String toString()  {
			return " PN=" + this.proposalNumber;
		}
	}
	
	public static class PrepareRequest extends Request {
		
		public PrepareRequest(int pn) {
			super(pn);
		}
		
		public String toString() {
			return "PrepareRequest " + super.toString();
		}
		
	}
	
	/**
	 * There are 3 things we need to encapsulate in the responses for Prepare
	 * request:
	 * - participant may have received this as the very first Prepare request
	 * 		so the participant promises to accept this proposal when offer is made;
	 * - participant has responded to other at least one another Prepare request
	 * 		even though she has not accepted any proposal;
	 * - participant as accepted at least one proposal.
	 *
	 */
	public enum PrepareResponseState {
		
		/**
		 * When a participant is going to reject a prepare request.
		 * Actually we never send that as a response, so it is only
		 * hypothetical state; the default state Prepare Response is.
		 */
		REJECTED,
		
		/**
		 * Participant is responding the prepare request with the promise
		 * of accepting this particular proposal from the Proposer (provided it
		 * is the highest ranking proposal at that time). But this is the very
		 * first proposal the participant has come across, so neither any other
		 * Prepare Request responded / promised nor any proposal accepted.
		 */
		NONE_PROMISED_OR_ACCEPTED,
		
		/**
		 * Participant is responding the prepare request with the promise
		 * of accepting this particular proposal from the Proposer (provided it
		 * is the highest ranking proposal at that time). So far the participant
		 * has promised to at least one other proposal but have not accepted
		 * any. Since the incoming proposal is higher ranked than the earlier
		 * which the participant responded, it is promising that it will accept
		 * this proposal when it comes (provided it is the highest ranked.)
		 */
		PROMISED_NONE_ACCEPTED,
		
		/**
		 * Participant is responding the prepare request with the promise
		 * of accepting this particular proposal from the Proposer (provided it
		 * is the highest ranking proposal at that time). So far the participant
		 * has accepted at least one proposal. In the response, details of the
		 * accepted proposal - it's rank and value - will be provided.
		 */
		ACCEPTED;
	}
	
	public static class PrepareResponse {
		
		private PrepareResponseState state;
		private int lastPreparedRequestProposalNo;
		private int lastAcceptedProposal;
		private int lastAcceptedProposalValue;
		
		public PrepareResponse() {
			state = PrepareResponseState.REJECTED;
			lastAcceptedProposal = Participant.NO_PROPOSAL_ACCEPTED_YET;
			lastAcceptedProposalValue = Participant.PROPOSAL_VALUE_NOT_APPLICABLE;
			lastPreparedRequestProposalNo = Participant.NO_PROPOSAL_ACCEPTED_YET;
		}

		public PrepareResponseState getState() {
			return state;
		}

		public void setState(PrepareResponseState state) {
			this.state = state;
		}

		public int getLastPreparedRequestProposalNo() {
			if (state == PrepareResponseState.PROMISED_NONE_ACCEPTED || state == PrepareResponseState.ACCEPTED) {
				return lastPreparedRequestProposalNo;
			} else {
				throw new InvalidPrepareResponseOperation("Either Prepare Request is Rejected or it is the first request, hence no last prepare request proposal number.");
			}
		}

		public void setLastPreparedRequestProposalNo(int lastPreparedRequestProposalNo) {
			if (state == PrepareResponseState.REJECTED || 
					state == PrepareResponseState.NONE_PROMISED_OR_ACCEPTED || 
							state == PrepareResponseState.PROMISED_NONE_ACCEPTED) {
				this.lastPreparedRequestProposalNo = lastPreparedRequestProposalNo;
				this.state = PrepareResponseState.PROMISED_NONE_ACCEPTED;
			} else {
				throw new InvalidPrepareResponseOperation("Proposal is already accepted.");
			}
		}

		public int getLastAcceptedProposal() {
			if (state == PrepareResponseState.ACCEPTED) {
				return lastAcceptedProposal;
			} else {
				throw new InvalidPrepareResponseOperation("No proposal is accepted yet.");
			}
		}

		public void setLastAcceptedProposal(int lastAcceptedProposal) {
			if (state == PrepareResponseState.PROMISED_NONE_ACCEPTED || state == PrepareResponseState.ACCEPTED) {
				this.lastAcceptedProposal = lastAcceptedProposal;
				this.state = PrepareResponseState.ACCEPTED;
			} else {
				throw new InvalidPrepareResponseOperation("No Prepare request is promised yet.");
			}
		}
		
		// Proposal need to be accepted before value of the accepted proposal is set.

		public int getLastAcceptedProposalValue() {
			if (state == PrepareResponseState.ACCEPTED) {
				return lastAcceptedProposalValue;
			} else {
				throw new InvalidPrepareResponseOperation("Either no proposal is promised or accepted yet.");
			}
		}

		public void setLastAcceptedProposalValue(int lastAcceptedProposalValue) {
			if (state == PrepareResponseState.ACCEPTED) {
				this.lastAcceptedProposalValue = lastAcceptedProposalValue;
			} else {
				throw new InvalidPrepareResponseOperation("No proposal is accepted yet");
			}
		}

		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("{PrepareResponse " + state);
			switch (this.state) {
			
			case REJECTED:
				break;
				
			case NONE_PROMISED_OR_ACCEPTED:
				// receiving the first proposal
				break;
				
			case PROMISED_NONE_ACCEPTED:
				sb.append(" Last responded prepared request proposal number: " + lastPreparedRequestProposalNo);
				break;
				
			case ACCEPTED:
				sb.append(" Last accepted proposal no.: " + lastAcceptedProposal + " Last accepted proposal value: " + lastAcceptedProposalValue);
				break;
			}
			sb.append("}");
			return sb.toString();
		}
		
	}
	
	@SuppressWarnings("serial")
	public static class InvalidPrepareResponseOperation extends RuntimeException {
		
		public InvalidPrepareResponseOperation(String msg) {
			super(msg);
		}
		
	}
	
	public static class AcceptRequest extends Request {
		
		public final int proposalValue;
		
		public AcceptRequest(int pn, int val) {
			super(pn);
			proposalValue = val;
		}
		
		public String toString()  {
			return "AcceptRequest " + super.toString() + " PV=" + this.proposalValue;
		}
		
	}
	
	public static class AcceptResponse {
		
		public AcceptResponse() {
			// basically consent for the accept request
		}
		
		public String toString() {
			return "AcceptResponse";
		}
		
	}

}
