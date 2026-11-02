/**
 * The network's claim is that a task can reach what it needs and nothing else.
 *
 * That claim is one security group rule away from false, and the difference does not show up in
 * anything a deployed fleet does — a task with open egress works exactly as well as one without.
 * So it is asserted here.
 *
 * It was also one route away from false, and that is the more useful lesson. This suite asserted
 * the firewall's rules, its drop-by-default action, its deletion protections, and its existence,
 * and every one of those passed while traffic bypassed it entirely. Configuration was never the
 * problem; being in the path was. An assertion about a control's settings is not an assertion
 * that the control is reached.
 */

import { Annotations, Match, Template } from 'aws-cdk-lib/assertions';
import { NetworkStack } from '../lib/network-stack';
import { deployedContext, fleetStacks, testApp } from './helpers';

let template: Template;

beforeAll(() => {
  template = Template.fromStack(new NetworkStack(testApp(), 'TestNetwork'));
});

/**
 * CDK represents "this group allows no egress" as an unreachable ICMP rule to 255.255.255.255/32.
 * It permits nothing, so counting it as a real rule would make every assertion below fail on a
 * group that is in fact closed.
 */
type Resource = { [key: string]: any };

function realEgressRules(group: Resource): Record<string, any>[] {
  return (group.Properties.SecurityGroupEgress ?? []).filter(
    (rule: Record<string, any>) => rule.CidrIp !== '255.255.255.255/32',
  );
}

function groupDescribedAs(fragment: string): Resource {
  const groups = Object.values(template.findResources('AWS::EC2::SecurityGroup'));
  const match = groups.find((g) => String(g.Properties.GroupDescription ?? '').includes(fragment));

  if (!match) {
    throw new Error(`no security group described as ${fragment}`);
  }
  return match;
}

/**
 * Service names synthesize as `Fn::Join`, not strings, so a regex match against the value fails
 * however the name is spelled. Comparing the rendered join is the reliable form.
 */
function endpointServiceNames(type: 'Interface' | 'Gateway'): string[] {
  return Object.values(template.findResources('AWS::EC2::VPCEndpoint'))
    .filter((endpoint) => (endpoint.Properties.VpcEndpointType ?? 'Gateway') === type)
    .map((endpoint) => JSON.stringify(endpoint.Properties.ServiceName));
}

function hasEndpointFor(type: 'Interface' | 'Gateway', service: string): boolean {
  return endpointServiceNames(type).some((name) => name.includes(`.${service}"`));
}

describe('tasks are not reachable and cannot roam', () => {
  test('task subnets do not assign public addresses', () => {
    const subnets = template.findResources('AWS::EC2::Subnet');
    // 'Isolated' as well as 'Private': the tasks' tier is isolated in the default mode, and
    // filtering only on 'Private' matched nothing and still passed the loop below.
    const privateSubnets = Object.values(subnets).filter((s) =>
      (s.Properties.Tags ?? []).some(
        (t: { Key: string; Value: string }) =>
          t.Key === 'aws-cdk:subnet-type' && ['Private', 'Isolated'].includes(t.Value),
      ),
    );

    expect(privateSubnets.length).toBeGreaterThan(0);
    for (const subnet of privateSubnets) {
      expect(subnet.Properties.MapPublicIpOnLaunch).toBeFalsy();
    }
  });

  test('no security group allows every protocol outbound', () => {
    // CDK's default is allow-all outbound, so this property is one omitted argument from false.
    for (const group of Object.values(template.findResources('AWS::EC2::SecurityGroup'))) {
      for (const rule of realEgressRules(group)) {
        expect(rule.IpProtocol).not.toBe('-1');
      }
    }
  });

  test('outbound is HTTPS only', () => {
    for (const group of Object.values(template.findResources('AWS::EC2::SecurityGroup'))) {
      for (const rule of realEgressRules(group)) {
        expect(rule.IpProtocol).toBe('tcp');
        expect(rule.FromPort).toBe(443);
        expect(rule.ToPort).toBe(443);
      }
    }
  });

  test('nothing may reach a task', () => {
    expect(groupDescribedAs('Worker tasks').Properties.SecurityGroupIngress ?? []).toHaveLength(0);
  });

  test('the endpoints accept traffic only from inside the VPC', () => {
    const groups = template.findResources('AWS::EC2::SecurityGroup');
    const endpointGroup = Object.values(groups).find((g) =>
      String(g.Properties.GroupDescription ?? '').includes('Interface endpoints'),
    );

    expect(endpointGroup).toBeDefined();
    for (const rule of endpointGroup!.Properties.SecurityGroupIngress ?? []) {
      expect(rule.CidrIp).not.toBe('0.0.0.0/0');
    }
  });
});

describe('the AWS calls stay on PrivateLink', () => {
  test('object storage and tables use gateway endpoints', () => {
    // Free and routed in the route table, so there is no reason not to have them.
    expect(hasEndpointFor('Gateway', 's3')).toBe(true);
    expect(hasEndpointFor('Gateway', 'dynamodb')).toBe(true);
  });

  // Only the first two are justified by volume. The rest are bought for exposure, each keeping
  // a call that carries a credential or a result off the public internet for about $14.60 a
  // month. Distinguished here because the suite used to call all of them high-volume, and a
  // capture of one run measured 0.39 MB across every destination combined.
  test.each([
    ['ecr.api', 'image metadata', 'volume'],
    ['ecr.dkr', 'image layers, 0.69 GB per host', 'volume'],
    ['sqs', 'work items', 'exposure'],
    ['secretsmanager', 'subscription keys', 'exposure'],
    ['logs', 'task output', 'exposure'],
    ['sts', 'role assumption', 'exposure'],
    ['q', 'measured at 4% of a run, and not the inference path', 'exposure'],
  ])('%s has an interface endpoint (%s, justified by %s)', (service) => {
    expect(hasEndpointFor('Interface', service)).toBe(true);
  });

  test('every interface endpoint resolves privately', () => {
    // Without private DNS the SDK still resolves the public name and the endpoint is unused, which
    // is a bill with no benefit.
    const endpoints = template.findResources('AWS::EC2::VPCEndpoint');
    const interfaces = Object.values(endpoints).filter(
      (e) => e.Properties.VpcEndpointType === 'Interface',
    );

    expect(interfaces.length).toBe(7);
    for (const endpoint of interfaces) {
      expect(endpoint.Properties.PrivateDnsEnabled).toBe(true);
    }
  });

  test('the inference path is not among them, because it cannot be', () => {
    // The correction that cost the most to establish. The design assumed inference rode
    // PrivateLink through `codewhisperer` or `q`; a capture showed it going to
    // runtime.<region>.kiro.dev, which is not an AWS service and has no endpoint. So the
    // largest ongoing flow — 59% of a run — necessarily crosses the restricted egress path,
    // and the endpoints above are justified by the image pull instead.
    const domains = deployedContext()['fleet:kiroEgressDomains'] as string[];

    expect(domains.some((domain) => domain.startsWith('runtime.'))).toBe(true);
  });
});

describe('by default there is no path out to constrain', () => {
  test('no NAT gateway, and no internet gateway either', () => {
    // The saving, and the control. Three earlier versions of this stack argued about how to
    // restrict a path to the internet; this one does not have the path. A deployment established
    // that every destination the workload needs resolves inside the VPC, the Kiro service
    // included, which is what makes the question disappear rather than get answered.
    template.resourceCountIs('AWS::EC2::NatGateway', 0);
    template.resourceCountIs('AWS::EC2::InternetGateway', 0);
  });

  test('the task subnets have no route off the VPC', () => {
    // Stronger than any allowlist, and the thing the earlier firewall only appeared to do. A
    // route table with no default entry cannot be bypassed, misconfigured, or billed.
    const routes = Object.values(template.findResources('AWS::EC2::Route')).filter(
      (route) => route.Properties.DestinationCidrBlock === '0.0.0.0/0',
    );

    expect(routes).toHaveLength(0);
  });

  test('no firewall, of either kind', () => {
    // Nothing to inspect, so nothing to pay for. Network Firewall came to about $288 a month
    // standing at one endpoint; DNS Firewall was tried and dropped because it also evaluates
    // the interface endpoints' private DNS, so making it work needed `*.amazonaws.com` — which
    // permits an attacker's own bucket and is a wider allowlist than no internet at all.
    template.resourceCountIs('AWS::NetworkFirewall::Firewall', 0);
    template.resourceCountIs('AWS::Route53Resolver::FirewallRuleGroupAssociation', 0);
  });

  test('the only public subnet is the one that does not exist', () => {
    const subnets = template.findResources('AWS::EC2::Subnet');
    const public_ = Object.values(subnets).filter((subnet) =>
      (subnet.Properties.Tags ?? []).some(
        (tag: { Key: string; Value: string }) =>
          tag.Key === 'aws-cdk:subnet-type' && tag.Value === 'Public',
      ),
    );

    expect(public_).toHaveLength(0);
  });

  test('the tasks may reach the VPC and nothing beyond it', () => {
    // In this mode a security group audit and the route table agree, which is the property the
    // earlier design could not claim: there, the group permitted 0.0.0.0/0 on 443 and the
    // destination control lived somewhere else entirely.
    const rules = realEgressRules(groupDescribedAs('Worker tasks'));

    expect(rules).toHaveLength(1);
    expect(rules[0]).toMatchObject({ IpProtocol: 'tcp', FromPort: 443, ToPort: 443 });
    // The VPC's own CIDR, resolved at deploy time, and specifically not 0.0.0.0/0.
    expect(JSON.stringify(rules[0].CidrIp)).toContain('CidrBlock');
  });

  test('the Kiro service is reached through the q endpoint', () => {
    // The finding the default rests on, and it reversed two earlier conclusions. The `q`
    // interface endpoint serves private DNS for `management`, `runtime` and `telemetry` under
    // `kiro.dev` — confirmed from `describe-vpc-endpoints` on a deployed VPC, where both Kiro
    // names resolved to that endpoint's ENI. So inference does ride PrivateLink, and the claim
    // that `*.kiro.dev` has no PrivateLink equivalent was wrong.
    //
    // Which makes this endpoint load-bearing rather than incidental: without it the fleet cannot
    // reach a model at all in this mode.
    expect(hasEndpointFor('Interface', 'q')).toBe(true);
  });
});
describe("egressControl 'inspection': server-name matching, at an hourly price", () => {
  let inspected: Template;

  beforeAll(() => {
    inspected = Template.fromStack(
      fleetStacks({ 'fleet:egressControl': 'inspection' }).network,
    );
  });

  test('the allowlist matches TLS server names, not only HTTP hosts', () => {
    // Everything here is HTTPS, so an HTTP_HOST-only rule would match nothing while appearing
    // configured.
    inspected.hasResourceProperties('AWS::NetworkFirewall::RuleGroup', {
      Type: 'STATEFUL',
      RuleGroup: Match.objectLike({
        RulesSource: {
          RulesSourceList: Match.objectLike({
            GeneratedRulesType: 'ALLOWLIST',
            TargetTypes: Match.arrayWith(['TLS_SNI']),
            Targets: Match.arrayWith([Match.stringLikeRegexp('kiro\\.dev$')]),
          }),
        },
      }),
    });
  });

  test('anything not on the list is dropped', () => {
    inspected.hasResourceProperties('AWS::NetworkFirewall::FirewallPolicy', {
      FirewallPolicy: Match.objectLike({
        StatefulDefaultActions: Match.arrayWith(['aws:drop_strict']),
      }),
    });
  });

  test('the tasks actually route through it', () => {
    // The assertion this suite was missing, and its absence let a real defect ship. Every other
    // property of the firewall was asserted — rules, default action, deletion protections, that
    // the resource existed — and none said whether traffic went through it. It did not: CDK
    // writes a default route to the NAT gateway for a PRIVATE_WITH_EGRESS subnet, so the tasks
    // left the VPC without passing anything, and the firewall was an expensive ornament.
    const routes = Object.values(inspected.findResources('AWS::EC2::Route')).filter(
      (route) => route.Properties.DestinationCidrBlock === '0.0.0.0/0',
    );

    const viaFirewall = routes.filter((route) => route.Properties.VpcEndpointId);
    expect(viaFirewall).toHaveLength(2);
    for (const route of viaFirewall) {
      expect(JSON.stringify(route.Properties.VpcEndpointId)).toContain('EgressFirewall');
    }
  });

  test('one firewall endpoint, for the reason there is one gateway', () => {
    // $0.395 an hour each, accruing at zero tasks. Two came to roughly $577 a month, on a
    // pattern whose argument is that it costs nothing when idle.
    const firewall = Object.values(
      inspected.findResources('AWS::NetworkFirewall::Firewall'),
    )[0];

    expect(firewall.Properties.SubnetMappings).toHaveLength(1);
  });

  test('it cannot be removed or reconfigured by accident', () => {
    inspected.hasResourceProperties('AWS::NetworkFirewall::Firewall', {
      DeleteProtection: true,
      FirewallPolicyChangeProtection: true,
      SubnetChangeProtection: true,
    });
  });
});

describe("egressControl 'open': says what it gives up", () => {
  test('choosing it warns that destination control is gone', () => {
    // It must not be possible to take the cheap option without being told that "restricted
    // egress" has become "port 443".
    const relaxed = fleetStacks({ 'fleet:egressControl': 'open' });

    Annotations.fromStack(relaxed.network).hasWarning(
      '*',
      Match.stringLikeRegexp('any host on 443'),
    );
    const open = Template.fromStack(relaxed.network);
    open.resourceCountIs('AWS::NetworkFirewall::Firewall', 0);
    // It does get a gateway, which 'private' does not. That is the cost of the choice.
    open.resourceCountIs('AWS::EC2::NatGateway', 1);
  });
});

describe('configuration that must not be guessed', () => {
  test('an empty allowlist is refused rather than deployed', () => {
    // An empty list produces a fleet that cannot authenticate, and it fails at task start rather
    // than at deploy, which is the expensive place to find out.
    // Only meaningful where there is an allowlist to be empty. The default mode has no egress
    // path and so no list, which is why this names 'inspection'.
    const app = testApp({
      'fleet:kiroEgressDomains': [],
      'fleet:egressControl': 'inspection',
    });

    expect(() => Template.fromStack(new NetworkStack(app, 'NoDomains'))).toThrow(
      /kiroEgressDomains is empty/,
    );
  });
});
