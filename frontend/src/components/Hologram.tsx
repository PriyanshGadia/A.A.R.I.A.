// src/components/Hologram.tsx
import * as THREE from "three";
import { Suspense, useMemo, useRef } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import { Line } from "@react-three/drei";
import { SystemState, HologramNode, HologramLink } from "../hooks/useAariaSocket";

// Small helper: safe color conversion with fallback
function safeColor(hex: string, fallback = "#ffffff") {
  try {
    return new THREE.Color(hex);
  } catch {
    return new THREE.Color(fallback);
  }
}

type GlowNodeProps = {
  node: HologramNode;
  position: THREE.Vector3;
};

export function GlowNode({ node, position }: GlowNodeProps) {
  const mesh = useRef<THREE.Mesh | null>(null);
  const light = useRef<THREE.PointLight | null>(null);

  const baseColor = useMemo(() => safeColor(node.color || "#ffffff", "#ffffff"), [node.color]);
  const white = useMemo(() => new THREE.Color("#ffffff"), []);
  const black = useMemo(() => new THREE.Color("#000000"), []);
  const red = useMemo(() => new THREE.Color("#ff0000"), []);

  // target color depends on state
  const targetColor = useMemo(() => {
    if (node.state === "spawning") return white;
    if (node.state === "error") return red;
    if (node.state === "deactivated") return black;
    return baseColor;
  }, [node.state, baseColor]);

  // a small local clock for per-node offsets
  const offset = useMemo(() => (Number(node.spawn_time || 0) % 1000) / 1000, [node.spawn_time]);

  useFrame((state) => {
    if (!mesh.current) return;
    // material may be an array or single material; guard it
    const material = Array.isArray(mesh.current.material) ? mesh.current.material[0] : mesh.current.material;
    const mat = material as THREE.MeshStandardMaterial;

    if (!mat) return;

    // smooth color transition
    mat.color.lerp(targetColor, 0.06);
    mat.emissive.lerp(targetColor, 0.06);

    // breathing and flicker: two-layer noise
    const t = state.clock.elapsedTime + offset;
    let intensity = 0.45;
    if (node.state === "active") {
      intensity = 0.9 + 0.4 * Math.sin(t * 20 + offset * 10); // fast flicker
    } else if (node.state === "idle") {
      intensity = 0.4 + 0.12 * Math.sin(t * 2 + offset * 2);
    } else if (node.state === "spawning") {
      intensity = 0.9 * (1 - Math.max(0, (node.spawn_time ? (state.clock.elapsedTime - node.spawn_time) : 0)));
    } else if (node.state === "error") {
      intensity = 1.4 * (0.7 + 0.6 * Math.abs(Math.sin(t * 6))); // staccato red pulse
    } else if (node.state === "deactivated") {
      intensity = Math.max(0.02, 0.4 * (1 - Math.min(1, (state.clock.elapsedTime - (node.spawn_time || 0)) / 1.2)));
    }

    // lerp material emissiveIntensity to target
    mat.emissiveIntensity = THREE.MathUtils.lerp(mat.emissiveIntensity ?? 0.5, intensity, 0.08);
    if (light.current) {
      light.current.intensity = THREE.MathUtils.lerp(light.current.intensity ?? 0.3, intensity * 1.6, 0.08);
    }
  });

  return (
    <group position={position.toArray()}>
      <mesh ref={mesh}>
        <sphereGeometry args={[Math.max(0.04, (node.size || 6) * 0.01), 12, 12]} />
        <meshStandardMaterial color={"#ffffff"} emissive={"#ffffff"} emissiveIntensity={0.5} metalness={0.1} roughness={0.5} />
      </mesh>
      <pointLight
        ref={light}
        color={node.color || "#ffffff"}
        intensity={0.8}
        distance={Math.max(0.6, (node.size || 6) * 0.12)}
        decay={2}
      />
    </group>
  );
}

type GradientLinkProps = {
  link: HologramLink;
  start: THREE.Vector3;
  end: THREE.Vector3;
  startColor: THREE.Color;
  endColor: THREE.Color;
};

export function GradientLink({ link, start, end, startColor }: GradientLinkProps) {
  const lineRef = useRef<any | null>(null);

  // animate opacity & width based on link.state/intensity
  useFrame(() => {
    if (lineRef.current) {
      const mat = lineRef.current.material as any;
      const targetOpacity = Math.min(1, 0.2 + (link.intensity ?? 0) * 2.5 + (link.state === "active" ? 0.25 : 0));
      mat.opacity = THREE.MathUtils.lerp(mat.opacity ?? 0.3, targetOpacity, 0.06);
    }
  });

  // Drei Line accepts color as string — convert
  const colorString = startColor.getStyle();

  return (
    <Line
      ref={lineRef}
      points={[start, end]}
      color={colorString}
      lineWidth={Math.max(0.002, (link.intensity ?? 0) * 0.01)}
      transparent
    />
  );
}

// traveling dots: small instanced spheres moving along each link
export function LinkDots({ link, start, end }: { link: HologramLink; start: THREE.Vector3; end: THREE.Vector3 }) {
  const group = useRef<THREE.Group | null>(null);
  const dotCount = Math.max(1, Math.floor(link.dot_density ?? 2));
  const colors = useMemo(() => {
    const c0 = safeColor(link.gradient?.[0] || "#ffffff");
    const c1 = safeColor(link.gradient?.[1] || "#999999");
    return [c0, c1];
  }, [link.gradient]);

  useFrame((state) => {
    if (!group.current) return;
    const now = state.clock.elapsedTime;
    const dir = new THREE.Vector3().subVectors(end, start);
    const length = dir.length();
    if (length === 0) return;
    dir.normalize();
    for (let i = 0; i < dotCount; i++) {
      const child = group.current.children[i] as THREE.Mesh | undefined;
      if (!child) continue;
      const speed = 0.3 + (link.intensity ?? 0.1) * 1.2;
      const offset = (i / dotCount) * 0.4;
      const progress = ((now * speed) + offset) % 1;
      const pos = new THREE.Vector3().copy(start).add(dir.clone().multiplyScalar(length * progress));
      child.position.copy(pos);
      // color lerp between gradient endpoints by progress
      const col = colors[0].clone().lerp(colors[1], progress);
      const mat = (child.material as THREE.MeshStandardMaterial | undefined);
      if (mat) {
        mat.color.copy(col);
        mat.emissive.copy(col);
      }
    }
  });

  return (
    <group ref={group}>
      {Array.from({ length: dotCount }).map((_, i) => (
        <mesh key={i}>
          <sphereGeometry args={[Math.max(0.01, ((link.intensity ?? 0.05) * 0.03)), 6, 6]} />
          <meshStandardMaterial color={"#ffffff"} emissive={"#ffffff"} emissiveIntensity={1} toneMapped={false} />
        </mesh>
      ))}
    </group>
  );
}

export function Hologram({ systemState }: { systemState: SystemState }) {
  const { nodes = [], links = [] } = systemState || { nodes: [], links: [] };

  // core layout: pick fixed positions for named cores, dynamic around them
  const corePositions = useMemo(() => {
    return new Map<string, THREE.Vector3>([
      ["CognitionCore", new THREE.Vector3(0, 0.8, 0)],
      ["PersonaCore", new THREE.Vector3(-1.6, -0.2, -0.4)],
      ["AutonomyCore", new THREE.Vector3(1.6, -0.3, -0.4)],
      ["SecurityCore", new THREE.Vector3(0.0, -0.5, 1.6)],
      ["Memory", new THREE.Vector3(0.0, -0.5, -1.8)]
    ]);
  }, []);

  // map nodes to positions
  const nodeMap = useMemo(() => {
    const map = new Map<string, { node: HologramNode; pos: THREE.Vector3 }>();
    const counts = new Map<string, number>();

    // place core nodes
    nodes.forEach(n => {
      if (corePositions.has(n.id)) {
        const p = corePositions.get(n.id);
        if (p) map.set(n.id, { node: n, pos: p.clone() });
      }
    });

    // dynamic nodes: place near their first linked source or random near center
    nodes.forEach(n => {
      if (map.has(n.id)) return;
      // find a link that has this as target or source
      const parentLink = links.find(l => l.target === n.id) || links.find(l => l.source === n.id);
      let pos = new THREE.Vector3(0, 0, 0);
      if (parentLink && map.has(parentLink.source)) {
        const parent = map.get(parentLink.source)!;
        const count = counts.get(parent.node.id) || 0;
        const angle = count * 0.9 + (count % 2 ? 0.5 : -0.3);
        const radius = 0.45 + (count * 0.06);
        pos = parent.pos.clone().add(new THREE.Vector3(Math.cos(angle) * radius, (Math.sin(angle * 1.2) * 0.05), Math.sin(angle) * radius));
        counts.set(parent.node.id, count + 1);
      } else {
        pos = new THREE.Vector3((Math.random() - 0.5) * 2.0, (Math.random() - 0.5) * 0.6, (Math.random() - 0.5) * 2.0);
      }
      map.set(n.id, { node: n, pos });
    });

    return map;
  }, [nodes, links, corePositions]);

  // prepare link render data
  const preparedLinks = useMemo(() => {
    return links.map(l => {
      const start = nodeMap.get(l.source);
      const end = nodeMap.get(l.target);
      if (!start || !end) return null;
      return {
        link: l,
        start: start.pos,
        end: end.pos,
        startColor: safeColor(start.node.color || "#ffffff"),
        endColor: safeColor(end.node.color || "#ffffff")
      };
    }).filter(Boolean) as { link: HologramLink; start: THREE.Vector3; end: THREE.Vector3; startColor: THREE.Color; endColor: THREE.Color }[];
  }, [links, nodeMap]);

  return (
    <div className="w-full h-full flex items-center justify-center rounded-lg overflow-hidden">
      <Canvas camera={{ position: [0, 2.2, 5], fov: 45 }} dpr={[1, 2]} linear>
        <ambientLight intensity={0.12} />
        <directionalLight position={[2, 5, 1]} intensity={0.25} />
        <Suspense fallback={null}>
          <group>
            {Array.from(nodeMap.values()).map(({ node, pos }) => (
              <GlowNode key={node.id} node={node} position={pos} />
            ))}

            {preparedLinks.map(({ link, start, end, startColor }) => (
              <group key={link.id}>
                <GradientLink link={link} start={start} end={end} startColor={startColor} endColor={startColor} />
                <LinkDots link={link} start={start} end={end} />
              </group>
            ))}
          </group>
        </Suspense>
      </Canvas>
    </div>
  );
}
