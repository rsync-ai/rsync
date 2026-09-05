"use client";

import React, { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { 
  Shield, 
  Eye, 
  EyeOff, 
  CheckCircle, 
  XCircle, 
  Clock, 
  AlertTriangle,
  Search,
  Filter,
  RefreshCw,
  FileText,
  Hash,
  Lock,
  Unlock,
  Settings,
  Activity
} from "lucide-react";
import { API_ENDPOINTS } from "@/lib/config/api";
import { authFetch } from "@/lib/api/auth-fetch";
import { captureWorkspace, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace";

// Types
interface PIIScanResult {
  id: string;
  pipeline_id: string;
  table_name: string;
  column_name: string;
  pii_type: string;
  confidence: number;
  detection_method: string;
  suggested_masking: string;
  approved_action?: string;
  approved_by?: string;
  approved_at?: string;
  created_at: string;
}

interface ApprovalRequest {
  id: string;
  pipeline_id: string;
  requested_by: string;
  columns: PIIColumnRequest[];
  justification: string;
  status: "pending" | "approved" | "denied" | "partial" | "expired";
  decided_by?: string;
  decided_at?: string;
  conditions?: string[];
  expires_at?: string;
  created_at: string;
}

interface PIIColumnRequest {
  table_name: string;
  column_name: string;
  pii_type: string;
  requested_action: string;
  hash_function?: string;
  approved?: boolean;
  decision?: string;
}

interface PIIPolicy {
  id: string;
  policy_type: "always_mask" | "require_approval" | "audit_required";
  pii_type: string;
  action: string;
  hash_function?: string;
  condition?: string;
  priority: number;
  enabled: boolean;
}

interface HashFunction {
  id: string;
  name: string;
  type: "builtin" | "python" | "api" | "lookup_table";
  description: string;
  reversible: boolean;
  enabled: boolean;
}

export default function PIIDashboardPage() {
  const [activeTab, setActiveTab] = useState("overview");
  const [scanResults, setScanResults] = useState<PIIScanResult[]>([]);
  const [approvalRequests, setApprovalRequests] = useState<ApprovalRequest[]>([]);
  const [policies, setPolicies] = useState<PIIPolicy[]>([]);
  const [hashFunctions, setHashFunctions] = useState<HashFunction[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedRequest, setSelectedRequest] = useState<ApprovalRequest | null>(null);
  const [showDecisionDialog, setShowDecisionDialog] = useState(false);
  const [decisionNotes, setDecisionNotes] = useState("");
  const [searchTerm, setSearchTerm] = useState("");
  const [filterPIIType, setFilterPIIType] = useState<string>("all");

  useEffect(() => {
    fetchData();
  }, []);

  // PII data is workspace-scoped; re-fetch when the active workspace changes
  // (header switcher / other tab) so the page never shows another workspace's
  // scan results, approvals, or policy counts. (R6)
  useEffect(
    () => onActiveWorkspaceChange(() => fetchData()),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  const fetchData = async () => {
    setLoading(true);
    // Four sequential round-trips — a switch part-way through would otherwise
    // leave the page showing a mix of both workspaces' PII findings.
    const isStale = captureWorkspace();
    try {
      // Fetch PII scan results
      const scanRes = await authFetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/pii/scan/results`, { cache: "no-store" });
      if (isStale()) return;
      if (scanRes.ok) {
        const data = await scanRes.json();
        setScanResults(data.results || []);
      }

      // Fetch approval requests
      const approvalRes = await authFetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/pii/approvals?status=pending`, { cache: "no-store" });
      if (isStale()) return;
      if (approvalRes.ok) {
        const data = await approvalRes.json();
        setApprovalRequests(data.requests || []);
      }

      // Fetch policies
      const policyRes = await authFetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/pii/policies`, { cache: "no-store" });
      if (isStale()) return;
      if (policyRes.ok) {
        const data = await policyRes.json();
        setPolicies(data.policies || []);
      }

      // Fetch hash functions
      const hashRes = await authFetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/hash-functions`, { cache: "no-store" });
      if (isStale()) return;
      if (hashRes.ok) {
        const data = await hashRes.json();
        setHashFunctions(data.functions || []);
      }
    } catch (error) {
      console.error("Failed to fetch PII data:", error);
    } finally {
      setLoading(false);
    }
  };

  const handleApprovalDecision = async (requestId: string, decision: "approved" | "denied") => {
    try {
      const res = await authFetch(`${API_ENDPOINTS.API_GATEWAY_URL}/api/v1/pii/approvals/${requestId}/decide`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          decision,
          notes: decisionNotes,
        }),
      });

      if (res.ok) {
        setApprovalRequests(prev => prev.filter(r => r.id !== requestId));
        setShowDecisionDialog(false);
        setDecisionNotes("");
        setSelectedRequest(null);
      }
    } catch (error) {
      console.error("Failed to submit decision:", error);
    }
  };

  const getPIITypeBadgeColor = (piiType: string) => {
    const colors: Record<string, string> = {
      email: "bg-blue-500/20 text-blue-400",
      ssn: "bg-red-500/20 text-red-400",
      credit_card: "bg-red-500/20 text-red-400",
      phone: "bg-yellow-500/20 text-yellow-400",
      name: "bg-green-500/20 text-green-400",
      address: "bg-purple-500/20 text-purple-400",
      password: "bg-red-600/20 text-red-500",
      bank_account: "bg-orange-500/20 text-orange-400",
    };
    return colors[piiType] || "bg-gray-500/20 text-gray-400";
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case "pending":
        return <Badge className="bg-yellow-500/20 text-yellow-400"><Clock className="w-3 h-3 mr-1" />Pending</Badge>;
      case "approved":
        return <Badge className="bg-green-500/20 text-green-400"><CheckCircle className="w-3 h-3 mr-1" />Approved</Badge>;
      case "denied":
        return <Badge className="bg-red-500/20 text-red-400"><XCircle className="w-3 h-3 mr-1" />Denied</Badge>;
      case "expired":
        return <Badge className="bg-gray-500/20 text-gray-400"><AlertTriangle className="w-3 h-3 mr-1" />Expired</Badge>;
      default:
        return <Badge>{status}</Badge>;
    }
  };

  const filteredScanResults = scanResults.filter(result => {
    const matchesSearch = 
      result.table_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      result.column_name.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesFilter = filterPIIType === "all" || result.pii_type === filterPIIType;
    return matchesSearch && matchesFilter;
  });

  const piiTypeCounts = scanResults.reduce((acc, result) => {
    acc[result.pii_type] = (acc[result.pii_type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  return (
    <div className="container mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold flex items-center gap-2">
            <Shield className="w-8 h-8 text-blue-500" />
            PII Management
          </h1>
          <p className="text-muted-foreground mt-1">
            Detect, protect, and manage personally identifiable information
          </p>
        </div>
        <Button onClick={fetchData} variant="outline">
          <RefreshCw className="w-4 h-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card className="bg-gradient-to-br from-blue-500/10 to-blue-600/5 border-blue-500/20">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">PII Columns Detected</p>
                <p className="text-3xl font-bold">{scanResults.length}</p>
              </div>
              <Eye className="w-10 h-10 text-blue-500 opacity-50" />
            </div>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-yellow-500/10 to-yellow-600/5 border-yellow-500/20">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Pending Approvals</p>
                <p className="text-3xl font-bold">{approvalRequests.filter(r => r.status === "pending").length}</p>
              </div>
              <Clock className="w-10 h-10 text-yellow-500 opacity-50" />
            </div>
          </CardContent>
        </Card>

        {/* Deliberately not green, and deliberately not a padlock: these policies are
            stored, not enforced (see the note on the Policies tab). The previous
            styling promised protection this table does not provide. */}
        <Card className="bg-gradient-to-br from-slate-500/10 to-slate-600/5 border-slate-500/20">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Stored Policies</p>
                <p className="text-3xl font-bold">{policies.filter(p => p.enabled).length}</p>
              </div>
              <Settings className="w-10 h-10 text-slate-500 opacity-50" />
            </div>
          </CardContent>
        </Card>

        <Card className="bg-gradient-to-br from-purple-500/10 to-purple-600/5 border-purple-500/20">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-muted-foreground">Hash Functions</p>
                <p className="text-3xl font-bold">{hashFunctions.length}</p>
              </div>
              <Hash className="w-10 h-10 text-purple-500 opacity-50" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Main Tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
        <TabsList className="grid grid-cols-5 w-full max-w-2xl">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="approvals">
            Approvals
            {approvalRequests.filter(r => r.status === "pending").length > 0 && (
              <Badge className="ml-2 bg-yellow-500 text-black">
                {approvalRequests.filter(r => r.status === "pending").length}
              </Badge>
            )}
          </TabsTrigger>
          <TabsTrigger value="policies">Policies</TabsTrigger>
          <TabsTrigger value="hashing">Hash Functions</TabsTrigger>
          <TabsTrigger value="audit">Audit Log</TabsTrigger>
        </TabsList>

        {/* Overview Tab */}
        <TabsContent value="overview" className="space-y-4">
          {/* PII Type Distribution */}
          <Card>
            <CardHeader>
              <CardTitle>PII Type Distribution</CardTitle>
              <CardDescription>Breakdown of detected PII types across your data</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-4">
                {Object.entries(piiTypeCounts).map(([type, count]) => (
                  <div key={type} className="flex items-center gap-2 p-3 rounded-lg bg-muted/50">
                    <Badge className={getPIITypeBadgeColor(type)}>{type}</Badge>
                    <span className="font-semibold">{count}</span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          {/* Detected PII Table */}
          <Card>
            <CardHeader>
              <div className="flex justify-between items-center">
                <div>
                  <CardTitle>Detected PII Columns</CardTitle>
                  <CardDescription>All columns identified as containing PII</CardDescription>
                </div>
                <div className="flex gap-2">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground w-4 h-4" />
                    <Input
                      placeholder="Search columns..."
                      className="pl-10 w-64"
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                    />
                  </div>
                  <Select value={filterPIIType} onValueChange={setFilterPIIType}>
                    <SelectTrigger className="w-40">
                      <Filter className="w-4 h-4 mr-2" />
                      <SelectValue placeholder="Filter type" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Types</SelectItem>
                      <SelectItem value="email">Email</SelectItem>
                      <SelectItem value="ssn">SSN</SelectItem>
                      <SelectItem value="phone">Phone</SelectItem>
                      <SelectItem value="credit_card">Credit Card</SelectItem>
                      <SelectItem value="name">Name</SelectItem>
                      <SelectItem value="address">Address</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Table</TableHead>
                    <TableHead>Column</TableHead>
                    <TableHead>PII Type</TableHead>
                    <TableHead>Confidence</TableHead>
                    <TableHead>Detection</TableHead>
                    <TableHead>Suggested Action</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredScanResults.map((result) => (
                    <TableRow key={result.id}>
                      <TableCell className="font-mono text-sm">{result.table_name}</TableCell>
                      <TableCell className="font-mono text-sm">{result.column_name}</TableCell>
                      <TableCell>
                        <Badge className={getPIITypeBadgeColor(result.pii_type)}>
                          {result.pii_type}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <div className="w-20 bg-muted rounded-full h-2">
                            <div 
                              className="bg-blue-500 h-2 rounded-full" 
                              style={{ width: `${result.confidence * 100}%` }}
                            />
                          </div>
                          <span className="text-sm">{(result.confidence * 100).toFixed(0)}%</span>
                        </div>
                      </TableCell>
                      <TableCell className="text-sm text-muted-foreground">
                        {result.detection_method}
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{result.suggested_masking}</Badge>
                      </TableCell>
                      <TableCell>
                        {result.approved_action ? (
                          <Badge className="bg-green-500/20 text-green-400">
                            <CheckCircle className="w-3 h-3 mr-1" />
                            {result.approved_action}
                          </Badge>
                        ) : (
                          <Badge className="bg-gray-500/20 text-gray-400">
                            Pending Review
                          </Badge>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Approvals Tab */}
        <TabsContent value="approvals" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Pending Approval Requests</CardTitle>
              <CardDescription>Review and decide on PII transfer requests</CardDescription>
            </CardHeader>
            <CardContent>
              {approvalRequests.filter(r => r.status === "pending").length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  <CheckCircle className="w-12 h-12 mx-auto mb-4 opacity-50" />
                  <p>No pending approval requests</p>
                </div>
              ) : (
                <div className="space-y-4">
                  {approvalRequests.filter(r => r.status === "pending").map((request) => (
                    <Card key={request.id} className="border-yellow-500/20">
                      <CardContent className="pt-6">
                        <div className="flex justify-between items-start">
                          <div className="space-y-2">
                            <div className="flex items-center gap-2">
                              <AlertTriangle className="w-5 h-5 text-yellow-500" />
                              <span className="font-semibold">PII Transfer Request</span>
                              {getStatusBadge(request.status)}
                            </div>
                            <p className="text-sm text-muted-foreground">
                              Pipeline: {request.pipeline_id}
                            </p>
                            <p className="text-sm">
                              <span className="text-muted-foreground">Requested by:</span>{" "}
                              {request.requested_by}
                            </p>
                            <p className="text-sm">
                              <span className="text-muted-foreground">Justification:</span>{" "}
                              {request.justification}
                            </p>

                            <div className="mt-4">
                              <p className="text-sm font-medium mb-2">Columns Requested:</p>
                              <div className="flex flex-wrap gap-2">
                                {request.columns.map((col, idx) => (
                                  <Badge key={idx} variant="outline" className="text-xs">
                                    {col.table_name}.{col.column_name} ({col.pii_type}) → {col.requested_action}
                                  </Badge>
                                ))}
                              </div>
                            </div>
                          </div>
                          <div className="flex gap-2">
                            <Button
                              variant="outline"
                              className="border-red-500/50 text-red-500 hover:bg-red-500/10"
                              onClick={() => {
                                setSelectedRequest(request);
                                setShowDecisionDialog(true);
                              }}
                            >
                              <XCircle className="w-4 h-4 mr-2" />
                              Deny
                            </Button>
                            <Button
                              className="bg-green-600 hover:bg-green-700"
                              onClick={() => {
                                setSelectedRequest(request);
                                setShowDecisionDialog(true);
                              }}
                            >
                              <CheckCircle className="w-4 h-4 mr-2" />
                              Approve
                            </Button>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Policies Tab */}
        <TabsContent value="policies" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex justify-between items-center">
                <div>
                  <CardTitle>PII Policies</CardTitle>
                  <CardDescription>
                    Stored policy records. These are not enforced by the pipeline runtime —
                    to actually mask a column, add the <code>mask_pii</code> transform to the
                    pipeline.
                  </CardDescription>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Policy Type</TableHead>
                    <TableHead>PII Type</TableHead>
                    <TableHead>Action</TableHead>
                    <TableHead>Condition</TableHead>
                    <TableHead>Priority</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {policies.map((policy) => (
                    <TableRow key={policy.id}>
                      <TableCell>
                        <Badge variant="outline">{policy.policy_type}</Badge>
                      </TableCell>
                      <TableCell>
                        <Badge className={getPIITypeBadgeColor(policy.pii_type)}>
                          {policy.pii_type === "*" ? "All Types" : policy.pii_type}
                        </Badge>
                      </TableCell>
                      <TableCell>{policy.action}</TableCell>
                      <TableCell className="text-sm text-muted-foreground">
                        {policy.condition || "-"}
                      </TableCell>
                      <TableCell>{policy.priority}</TableCell>
                      <TableCell>
                        {policy.enabled ? (
                          <Badge className="bg-gray-500/20 text-gray-300">Enabled</Badge>
                        ) : (
                          <Badge className="bg-gray-500/20 text-gray-400">Disabled</Badge>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Hash Functions Tab */}
        <TabsContent value="hashing" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex justify-between items-center">
                <div>
                  <CardTitle>Hash Functions</CardTitle>
                  <CardDescription>Built-in and custom hash functions for PII masking</CardDescription>
                </div>

              </div>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {hashFunctions.map((func) => (
                  <Card key={func.id} className="border-muted">
                    <CardContent className="pt-6">
                      <div className="flex items-start justify-between">
                        <div>
                          <h3 className="font-semibold flex items-center gap-2">
                            <Hash className="w-4 h-4" />
                            {func.name}
                          </h3>
                          <Badge variant="outline" className="mt-1">{func.type}</Badge>
                        </div>
                        {func.reversible ? (
                          <Unlock className="w-5 h-5 text-yellow-500" />
                        ) : (
                          <Lock className="w-5 h-5 text-green-500" />
                        )}
                      </div>
                      <p className="text-sm text-muted-foreground mt-3">{func.description}</p>
                      <div className="flex items-center justify-between mt-4">
                        <span className="text-xs text-muted-foreground">
                          {func.reversible ? "Reversible" : "One-way"}
                        </span>
                        {func.enabled ? (
                          <Badge className="bg-green-500/20 text-green-400">Active</Badge>
                        ) : (
                          <Badge className="bg-gray-500/20 text-gray-400">Disabled</Badge>
                        )}
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Audit Log Tab */}
        <TabsContent value="audit" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>PII Access Audit Log</CardTitle>
              <CardDescription>Track all PII access and transformation events</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-center py-8 text-muted-foreground">
                <Activity className="w-12 h-12 mx-auto mb-4 opacity-50" />
                <p>Audit log will be displayed here</p>
                <p className="text-sm mt-2">All PII access events are logged for compliance</p>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Decision Dialog */}
      <Dialog open={showDecisionDialog} onOpenChange={setShowDecisionDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Review PII Transfer Request</DialogTitle>
            <DialogDescription>
              Decide whether to approve or deny this PII transfer request
            </DialogDescription>
          </DialogHeader>
          
          {selectedRequest && (
            <div className="space-y-4">
              <Alert>
                <AlertTriangle className="w-4 h-4" />
                <AlertDescription>
                  This action will affect {selectedRequest.columns.length} column(s) containing sensitive data.
                </AlertDescription>
              </Alert>

              <div className="space-y-2">
                <Label>Decision Notes</Label>
                <Textarea
                  placeholder="Add any notes or conditions for this decision..."
                  value={decisionNotes}
                  onChange={(e) => setDecisionNotes(e.target.value)}
                />
              </div>
            </div>
          )}

          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDecisionDialog(false)}>
              Cancel
            </Button>
            <Button
              variant="outline"
              className="border-red-500/50 text-red-500"
              onClick={() => selectedRequest && handleApprovalDecision(selectedRequest.id, "denied")}
            >
              Deny Request
            </Button>
            <Button
              className="bg-green-600 hover:bg-green-700"
              onClick={() => selectedRequest && handleApprovalDecision(selectedRequest.id, "approved")}
            >
              Approve Request
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

