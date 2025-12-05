package com.uiuc.systems;

import java.io.Serializable;
import java.util.List;
public class LoadStateResponse implements Serializable {
        private final List<String> processedTupleIds;
        public LoadStateResponse(List<String> ids) {
            this.processedTupleIds = ids;
        }
        public List<String> getProcessedTupleIds() { return processedTupleIds; }
}
