using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using zero.interop.entangled.common.model.interop;

// ReSharper disable InconsistentNaming

namespace zero.interop.entangled.common.model
{
    public static unsafe class IoTransaction
    {
        public const int NUM_TRITS_SERIALIZED_TRANSACTION = 8019;
        public const int NUM_TRITS_SIGNATURE = 6561;
        public const int NUM_TRITS_ADDRESS = 243;
        public const int NUM_TRITS_VALUE = 81;
        public const int NUM_TRITS_OBSOLETE_TAG = 81;
        public const int NUM_TRITS_TIMESTAMP = 27;
        public const int NUM_TRITS_CURRENT_INDEX = 27;
        public const int NUM_TRITS_LAST_INDEX = 27;
        public const int NUM_TRITS_BUNDLE = 243;
        public const int NUM_TRITS_TRUNK = 243;
        public const int NUM_TRITS_BRANCH = 243;
        public const int NUM_TRITS_TAG = 81;
        public const int NUM_TRITS_ATTACHMENT_TIMESTAMP = 27;
        public const int NUM_TRITS_ATTACHMENT_TIMESTAMP_LOWER = 27;
        public const int NUM_TRITS_ATTACHMENT_TIMESTAMP_UPPER = 27;
        public const int NUM_TRITS_NONCE = 81;
        public const int NUM_TRITS_HASH = 243;

        public const int NUM_TRYTES_SERIALIZED_TRANSACTION = 2673;
        public const int NUM_TRYTES_SIGNATURE = 2187;
        public const int NUM_TRYTES_ADDRESS = 81;
        public const int NUM_TRYTES_VALUE = 27;
        public const int NUM_TRYTES_OBSOLETE_TAG = 27;
        public const int NUM_TRYTES_TIMESTAMP = 9;
        public const int NUM_TRYTES_CURRENT_INDEX = 9;
        public const int NUM_TRYTES_LAST_INDEX = 9;
        public const int NUM_TRYTES_BUNDLE = 81;
        public const int NUM_TRYTES_TRUNK = 81;
        public const int NUM_TRYTES_BRANCH = 81;
        public const int NUM_TRYTES_TAG = 27;
        public const int NUM_TRYTES_ATTACHMENT_TIMESTAMP = 9;
        public const int NUM_TRYTES_ATTACHMENT_TIMESTAMP_LOWER = 9;
        public const int NUM_TRYTES_ATTACHMENT_TIMESTAMP_UPPER = 9;
        public const int NUM_TRYTES_NONCE = 27;
        public const int NUM_TRYTES_HASH = 81;

        [DllImport("interop", CallingConvention = CallingConvention.Cdecl)]
        public static extern int transaction_deserialize_from_trits(out IoMarshalledTransaction transaction, sbyte* trits);
    }
}
