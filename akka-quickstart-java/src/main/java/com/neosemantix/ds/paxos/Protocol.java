/**
 * 
 */
package com.neosemantix.ds.paxos;

/**
 * @author umeshpatil
 *
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
	
	public static class PrepareResponse {
		
		public final int lastAcceptedProposal;
		public final int lastAcceptedProposalValue;
		
		public PrepareResponse(int pn, int v) {
			lastAcceptedProposal = pn;
			lastAcceptedProposalValue = v;
		}
		
		public String toString() {
			return "PrepareResponse " + "LA_PN=" + lastAcceptedProposal + " LA_PV=" + lastAcceptedProposalValue;
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
