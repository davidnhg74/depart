interface RiskHeatmapItem {
  name: string;
  risk: 'high' | 'medium' | 'low' | 'safe';
  construct_type: string;
}

interface RiskHeatmapProps {
  items: RiskHeatmapItem[];
}

export default function RiskHeatmap({ items }: RiskHeatmapProps) {
  const getRiskColor = (risk: string): string => {
    switch (risk) {
      case 'high':
        return 'bg-red-500 hover:bg-red-600';
      case 'medium':
        return 'bg-amber-500 hover:bg-amber-600';
      case 'low':
        return 'bg-yellow-400 hover:bg-yellow-500';
      case 'safe':
        return 'bg-green-500 hover:bg-green-600';
      default:
        return 'bg-gray-300 hover:bg-gray-400';
    }
  };

  const getRiskLabel = (risk: string): string => {
    switch (risk) {
      case 'high':
        return 'High Risk';
      case 'medium':
        return 'Medium Risk';
      case 'low':
        return 'Low Risk';
      case 'safe':
        return 'Safe';
      default:
        return 'Unknown';
    }
  };

  if (items.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500">
        <p>No items to display</p>
      </div>
    );
  }

  return (
    <div className="flex flex-wrap gap-2">
      {items.map((item, idx) => (
        <div
          key={idx}
          className={`w-16 h-16 rounded-lg flex items-center justify-center text-white font-bold text-xs text-center p-2 cursor-pointer transition-all ${getRiskColor(
            item.risk
          )}`}
          title={`${item.name}\n${item.construct_type}\n${getRiskLabel(item.risk)}`}
        >
          <span className="text-center line-clamp-2">{item.name}</span>
        </div>
      ))}

      {/* Legend */}
      <div className="w-full mt-4 pt-4 border-t border-gray-200">
        <div className="flex flex-wrap gap-4 justify-center text-sm">
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 bg-red-500 rounded"></div>
            <span className="text-gray-700">High Risk</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 bg-amber-500 rounded"></div>
            <span className="text-gray-700">Medium Risk</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 bg-yellow-400 rounded"></div>
            <span className="text-gray-700">Low Risk</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 bg-green-500 rounded"></div>
            <span className="text-gray-700">Safe</span>
          </div>
        </div>
      </div>
    </div>
  );
}
